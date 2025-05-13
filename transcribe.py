from functools import partial
from db_client import get_client
from config import SFTP_CREDENTIALS
import os, tempfile, time, paramiko, whisper, torch
from faster_whisper import WhisperModel

# Force torch.load to default to weights_only=True unless caller overrides it
torch_load_orig = torch.load

def torch_load_safe(*args, **kwargs):
    kwargs.setdefault("weights_only", True)
    return torch_load_orig(*args, **kwargs)
torch.load = torch_load_safe


DEVICE = "cuda"
MODEL_NAME = "fw_tiny"  # fw_base, fw_tiny, or oa_base

def oa_text_segments(model, mp3):
    r = model.transcribe(str(mp3), word_timestamps=False)
    return [(s['start'], s['end'], s['text']) for s in r['segments']]

def oa_text_segments_word_level(model, mp3):
    r = model.transcribe(
        str(mp3),
        word_timestamps=True          # ← get word info
    )
    seg_rows  = []
    word_rows = []

    for s in r["segments"]:
        seg_rows.append((s["start"], s["end"], s["text"]))

        # each word dict: {"word": "hello", "start": ..., "end": ...}
        for idx, w in enumerate(s.get("words", [])):
            word_rows.append((idx, w["start"], w["end"], w["word"]))

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
    seg_rows  = []
    word_rows = []

    for s in seg_iter:              # s is faster_whisper.Segment
        seg_rows.append((s.start, s.end, s.text))
        for idx, w in enumerate(s.words):
            word_rows.append((idx, w.start, w.end, w.word))

    return seg_rows, word_rows


MODELS = {
    "oa_base": dict(
        builder=partial(whisper.load_model, "base", device=DEVICE),
        runner=oa_text_segments,
        word_level_runner=oa_text_segments_word_level
    ),
    "fw_base": dict(
        builder=partial(WhisperModel, "base", device=DEVICE, compute_type="float16"),
        runner=fw_text_segments,
        word_level_runner=fw_text_segments_word_level
    ),
    "fw_tiny": dict(
        builder=partial(WhisperModel, "tiny.en", device=DEVICE, compute_type="float16"),
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
    if name not in _loaded:
        _loaded[name] = MODELS[name]["builder"]()   # build once
    return name, _loaded[name], MODELS[name]["word_level_runner"]


def transcribe_missing_episodes(client):
    # 1. Get episodes without a valid transcription
    episodes = [ep for ep in client.get_episodes()  # will be ordered as newest first
                if not is_valid_transcription(ep.get("transcript"))]
    if not episodes:
        print("Nothing to do – all episodes already have a transcription.")
        return

    episodes = episodes[:1]  # TODO delete this after testing
    print(f'transcribing {len(episodes)} episodes')

    t1 = time.perf_counter()

    # 2. Re‑use the same Whisper model and SFTP connection for the whole batch
    model, run_fn = get_model(MODEL_NAME)
    transport = paramiko.Transport((SFTP_CREDENTIALS["host"],
                                    SFTP_CREDENTIALS["port"]))
    transport.connect(username=SFTP_CREDENTIALS["username"],
                      password=SFTP_CREDENTIALS["password"])
    sftp = paramiko.SFTPClient.from_transport(transport)

    # 3. Process each episode
    with client.conn.cursor() as cur:
        for ep in episodes:
            remote_path = ep['audio_path']
            try:
                segments = download_and_transcribe(sftp, remote_path, run_fn, model)
            except FileNotFoundError:
                print(f"File not found on server → {remote_path}")
                continue
            except Exception as e:
                print(f"Failed to transcribe {remote_path}: {e}")
                continue
            # TODO insert each segment instead of full transcript.
            cur.execute(
                "UPDATE episodes SET transcript = %s WHERE id = %s",
                (transcript, ep["id"]),
            )
            print(f"✓ episode {ep['id']} updated")

    client.conn.commit()        # flush all updates
    sftp.close()
    transport.close()
    client.close()

    t2 = time.perf_counter()
    time_for_full_job = (t2-t1) * len(episodes) / 60 / 60
    print(time_for_full_job)




def word_level_insert(client, episode_id, seg_rows, word_rows):
    # TODO implement in transcribe_missing_episodes after testing diff methods
    """call for each episode"""
    seg_sql = """
    INSERT INTO transcript_segments
        (episode_id, seg_idx, start_s, end_s, text)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (episode_id, seg_idx) DO UPDATE
        SET start_s = EXCLUDED.start_s,
            end_s   = EXCLUDED.end_s,
            text    = EXCLUDED.text;
    """

    word_sql = """
    INSERT INTO transcript_words
        (seg_id, word_idx, start_s, end_s, word)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
    """

    with client.conn.cursor() as cur:
        for seg_idx, (st, et, txt) in enumerate(seg_rows):
            cur.execute(seg_sql, (episode_id, seg_idx, st, et, txt))
            seg_id = cur.fetchone()[0]  # if you RETURNING id

            for word_idx, st_w, et_w, word in word_rows:
                cur.execute(word_sql, (seg_id, word_idx, st_w, et_w, word))

def is_valid_transcription(text: str | None) -> bool:
    """Very simple validity test – expand later."""
    return isinstance(text, str) and len(text.strip()) > 1


def download_and_transcribe(sftp, remote_path, run_fn, model):
    """Stream a remote MP3 through a temp file → Whisper → transcript string."""
    fd, temp_path = tempfile.mkstemp(suffix=".mp3")
    os.close(fd)
    try:
        # stream SFTP → temp file (open in normal write mode)
        with open(temp_path, "wb") as dst:
            sftp.getfo(remote_path, dst)
        # **file is now closed**, FFmpeg can open it freely
        segments = run_fn(model, temp_path)  # expects run_fn to return list like [(start, end, text), etc.]
        return segments
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)      # clean up no matter what


if __name__ == "__main__":
    client = get_client()
    transcribe_missing_episodes(client)
    client.close()
