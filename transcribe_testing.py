from transcribe import get_word_level_model
from db_client import get_client
from config import SFTP_CREDENTIALS
import random
import paramiko
import subprocess
import os, tempfile
from moviepy.editor import AudioFileClip, TextClip, CompositeVideoClip, concatenate_videoclips, VideoFileClip
from moviepy.editor import ColorClip
import textwrap

import moviepy.config as mpc
mpc.change_settings({"IMAGEMAGICK_BINARY": "magick"}) # tells moviepy to use "imagemagick convert" instead of "convert"

MPV_PATH = r"C:\Users\mmjac\Downloads\mpv\mpv.exe"

def transcribe_test(client):
    transcripts = []
    # 1. Get episodes
    episodes = [ep for ep in client.get_episodes()]
    if not episodes:
        print("Nothing to do – all episodes already have a transcription.")
        return
    episodes = random.sample(episodes, 50)
    print(f'transcribing {len(episodes)} episodes')

    # 2. load models
    model_names = ["oa_base", "fw_base", "fw_tiny"]
    models = [get_word_level_model(model_name) for model_name in model_names]  # returns (name, model, run_fn)

    # 3. get SFTP connection
    transport = paramiko.Transport((SFTP_CREDENTIALS["host"],
                                    SFTP_CREDENTIALS["port"]))
    transport.connect(username=SFTP_CREDENTIALS["username"],
                      password=SFTP_CREDENTIALS["password"])
    sftp = paramiko.SFTPClient.from_transport(transport)

    for ep in episodes:
        remote_path = ep['audio_path']
        fd, temp_path = tempfile.mkstemp(suffix=".mp3")  # make temp file for full file
        os.close(fd)
        fd_clip, temp_path_clip = tempfile.mkstemp(suffix=".mp3")  # make temp file for clip
        os.close(fd_clip)
        # save from sftp to temp file
        with open(temp_path, "wb") as dst:
            sftp.getfo(remote_path, dst)

        # extract_clip(temp_path, 10, 10, temp_path_clip) # old method of extracting clip
        _ = extract_random_clip(temp_path, 15, temp_path_clip)

        try:
            episode_transcripts = {}
            episode_transcripts['clip_path'] = temp_path_clip  # need to save this so these clips can be merged later
            episode_transcripts['transcripts'] = {}
            for model_name, model, run_fn in models:
                segs, words = run_fn(model, episode_transcripts['clip_path'])

                # troubleshooting early preview:
                # word_text = " ".join(w[-1] for w in words)
                # print(model_name)
                # print("segs")
                # print(50*"-")
                # for i in segs:
                #     print(i)
                # print(50 * "-")
                # print("words")
                # print(word_text)

                text = " ".join(w[-1] for w in words)
                episode_transcripts['transcripts'][model_name] = text
            transcripts.append(episode_transcripts)
        except Exception as e:
            print('error occurred:', e)
            # delete both files if error occurs
            if os.path.exists(temp_path):
                os.remove(temp_path)
            if os.path.exists(temp_path_clip):
                os.remove(temp_path_clip)
        finally:
            # delete big temp file
            if os.path.exists(temp_path):
                os.remove(temp_path)

    episode_clips = []
    for episode_transcripts in transcripts:
        clip_path = episode_transcripts['clip_path']
        try:
            episode_clip = make_video_clip(clip_path, episode_transcripts['transcripts'])
            episode_clips.append(episode_clip)
        except Exception as e:
            print(f"Skipping clip due to error: {e}")

    final = concatenate_videoclips(episode_clips, method="compose")
    final.write_videofile("comparison_test.mp4", fps=24)
    for episode_transcripts in transcripts:
        clip_path = episode_transcripts['clip_path']
        # cleanup: delete clip files
        if os.path.exists(clip_path):
            os.remove(clip_path)

def extract_clip(input_path, start, duration, out_path):
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(start),
        "-t", str(duration),
        "-i", input_path,
        "-c", "copy",
        out_path
    ]
    subprocess.run(cmd, check=True)

def get_audio_duration(path):
    """Get audio duration in seconds using ffprobe."""
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(path)
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return float(result.stdout.strip())


def extract_random_clip(input_path, clip_length, output_path):
    duration = get_audio_duration(input_path)
    if duration <= clip_length:
        raise ValueError(f"Audio too short: {duration:.2f}s")

    start = random.uniform(0, duration - clip_length)
    extract_clip(input_path, start, clip_length, output_path)
    return start  # useful for tracking where the clip came from

def wrap_text(text, width=80):
    return "\n".join(textwrap.wrap(text, width=width))

def make_video_clip(audio_path, transcripts, width=1280, height=720):
    print(f"Creating video for: {audio_path}")
    if not os.path.isfile(audio_path):
        raise FileNotFoundError(f"Audio file not found: {audio_path}")
    try:
        audio = AudioFileClip(str(audio_path))
    except Exception as e:
        raise RuntimeError(f"Failed to load audio {audio_path}: {e}")
    clips = []
    y = 50
    for model, text in transcripts.items():
        # txt = f"{model.upper()}:\n{text}" # old: made 1 very long/small-font line of text
        # wrapped = f"{model.upper()}:\n{wrap_text(text, width=80)}"
        # clip = TextClip(wrapped, fontsize=28, color='white', size=(width-100, None)).set_position((50, y)).set_duration(audio.duration)

        wrapped = f"{model.upper()}:\n{wrap_text(text, width=80)}"
        clip = TextClip(
            wrapped,
            fontsize=28,
            color='white',
            size=(width - 100, None),
            method='caption'
        ).set_position((50, y)).set_duration(audio.duration)

        clips.append(clip)
        y += clip.size[1] + 20

        # clips.append(clip)
        # y += 150
    black_bg = ColorClip(size=(width, height), color=(0, 0, 0)).set_duration(audio.duration)
    return CompositeVideoClip([black_bg, *clips]).set_audio(audio)


def concat_video_clips(clip_paths, out_path="comparison_test.mp4", fps=24):
    """
    Concatenate multiple video clips into one video.

    clip_paths: List of paths to individual .mp4 files
    out_path: Output file path
    fps: Frames per second for output
    """
    clips = [VideoFileClip(str(p)) for p in clip_paths]
    final = concatenate_videoclips(clips, method="compose")
    final.write_videofile(out_path, fps=fps)


if __name__ == "__main__":
    client = get_client()
    transcribe_test(client)
