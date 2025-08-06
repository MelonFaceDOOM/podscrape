from functools import partial
import os
import tempfile
import whisper
import traceback
import torch
from faster_whisper import WhisperModel
from contextlib import ExitStack
from db_client import get_db_client
from sftp_client import get_sftp_client

# Force torch.load to default to weights_only=True unless caller overrides it
torch_load_orig = torch.load

# supress multi openmp warning
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"

# manually add to path in case you don't have admin rights to edit path normally
ffmpeg_path = "C:/ffmpeg/bin"
os.environ["PATH"] = ffmpeg_path + os.pathsep + os.environ["PATH"]

def torch_load_safe(*args, **kwargs):
    kwargs.setdefault("weights_only", True)
    return torch_load_orig(*args, **kwargs)

torch.load = torch_load_safe

DEVICE = "cuda"  # cuda or cpu
MODEL_NAME = "oa_base"  # fw_base, fw_tiny, or oa_base


def oa_text_segments(model, mp3):
    r = model.transcribe(str(mp3), word_timestamps=False)
    return [(s['start'], s['end'], s['text']) for s in r['segments']]


def oa_text_segments_word_level(model, mp3):
    r = model.transcribe(
        str(mp3),
        word_timestamps=True          # ← get word info
    )
    seg_rows = []
    word_rows = []

    for seg_idx, seg in enumerate(r["segments"]):
        seg_rows.append((seg["start"], seg["end"], seg["text"]))

        # each word dict: {"word": "hello", "start": ..., "end": ...}
        for word_idx, word in enumerate(seg.get("words", [])):
            word_rows.append(
                (seg_idx, word_idx, word["start"], word["end"], word["word"]))

    return seg_rows, word_rows


def fw_text_segments(model, mp3):
    segs, _ = model.transcribe(str(mp3), beam_size=1)
    return [(s.start, s.end, s.text) for s in segs]


def fw_text_segments_word_level_EXPERIMENTAL(model, mp3):
    # fw_tiny seems to have some problems with ending transcription early
    # possibly exactly after 1 segment?
    # this didn't fix but i'll experiment more later.
    seg_iter, _ = model.transcribe(
        str(mp3),
        beam_size=1,
        word_timestamps=True,
        temperature=[0.0, 0.2, 0.4],
        compression_ratio_threshold=2.4,
        log_prob_threshold=-1.0,
        no_speech_threshold=0.6,
        chunk_length=1800,
        max_new_tokens=None,
        vad_filter=False
    )
    seg_rows = []
    word_rows = []

    for s in seg_iter:
        seg_rows.append((s.start, s.end, s.text))
        for idx, w in enumerate(s.words):
            word_rows.append((idx, w.start, w.end, w.word))

    return seg_rows, word_rows


def fw_text_segments_word_level(model, mp3):
    seg_iter, _ = model.transcribe(
        str(mp3),
        beam_size=1,
        word_timestamps=True
    )
    seg_rows = []
    word_rows = []
    for seg_idx, seg in enumerate(seg_iter):  # s is faster_whisper.Segment
        seg_rows.append((seg.start, seg.end, seg.text))
        for word_idx, word in enumerate(seg.words):
            word_rows.append(
                (seg_idx, word_idx, word.start, word.end, word.word))
    return seg_rows, word_rows


MODELS = {
    "oa_base": dict(
        builder=partial(whisper.load_model, "base", device=DEVICE),
        runner=oa_text_segments,
        word_level_runner=oa_text_segments_word_level
    ),
    "fw_base": dict(
        builder=partial(WhisperModel, "base", device=DEVICE,
                        compute_type="float16"),
        runner=fw_text_segments,
        word_level_runner=fw_text_segments_word_level
    ),
    "fw_tiny": dict(
        builder=partial(WhisperModel, "tiny.en",
                        device=DEVICE, compute_type="float16"),
        runner=fw_text_segments,
        word_level_runner=fw_text_segments_word_level
    ),
}


_loaded = {}  # Cache of already-loaded models so we don't reload by accident


def get_model(name):
    if name not in _loaded:
        _loaded[name] = MODELS[name]["builder"]()   # build once
    return name, _loaded[name], MODELS[name]["runner"]


def get_word_level_model(name):
    # returns model, run_fn
    if name not in _loaded:
        _loaded[name] = MODELS[name]["builder"]()   # build once
    return _loaded[name], MODELS[name]["word_level_runner"]


def transcribe_missing_episodes():
    # 1. Open DB and SFTP connections so data can be transfered to/from each
    with ExitStack() as stack:
        db = stack.enter_context(get_db_client())
        sftp = stack.enter_context(get_sftp_client())

        # 2. Get episodes without a valid transcription
        episodes = db.get_episodes_with_no_transcript()
        # episodes will be ordered as newest first
        if not episodes:
            print("Nothing to do – all episodes already have a transcription.")
            return

        print(f'transcribing {len(episodes)} episodes')
        
        # 3. Load Whisper model
        model, run_fn = get_word_level_model(MODEL_NAME)

        # 4. Process each episode
        for ep in episodes:
            temp_path = None
            try:
                temp_path = save_ep_to_proj_folder(ep, sftp)

                try:
                    segs, words = run_fn(model, temp_path)  # transcribe
                except Exception as e:
                    print(f"Failed to transcribe {ep['audio_path']}: {e}")
                    traceback.print_exc()
                    continue

                db.word_level_insert(ep['id'], segs, words)
                print(f"episode {ep['audio_path']} updated")

            except KeyboardInterrupt:
                print("Interrupted by user. Cleaning up temp file and exiting...")
                if temp_path and os.path.exists(temp_path):
                    os.remove(temp_path)
                raise  # re-raise to exit cleanly

            except Exception as e:
                print(f"Failed to process episode {ep['audio_path']}: {e}")
                traceback.print_exc()
                continue

            finally:
                if temp_path and os.path.exists(temp_path):
                    os.remove(temp_path)


def save_ep_to_proj_folder(ep, sftp):
    remote_path = ep['audio_path']
    try:
        sftp.sftp.stat(remote_path)
    except FileNotFoundError:
        print(f"Remote file not found: {remote_path}")
        raise
    temp_dir = os.getcwd()
    temp_path = os.path.join(temp_dir, f"temp_{os.path.basename(remote_path)}")
    with open(temp_path, "wb") as dst:
        sftp.sftp.getfo(remote_path, dst)
    print(f"Path to transcribe: {temp_path}")
    return temp_path


if __name__ == "__main__":
    transcribe_missing_episodes()
