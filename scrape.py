import os
import time
import requests
from contextlib import ExitStack
from db_client import get_db_client
from sftp_client import get_sftp_client
from dotenv import load_dotenv

load_dotenv()

SFTP_PODCAST_FOLDER = os.getenv("SFTP_PODCAST_FOLDER")
LOCAL_SAVE_FOLDER = os.getenv("LOCAL_SAVE_FOLDER")

"""
Scrape podcasts.
Save audio file to either to local dir or to sftp server.
Save metadata to db.
"""


def download_episodes_and_save_remotely(episodes):
    """Take a list of episode dicts (with var names in RSS format,
        with audio_path and unique_id added
        NOT in DB format, which has some diff var names
        download the audio file, save it to sftp
        if that succeeds, save metadata to db"""
    with ExitStack() as stack:
        # db and sftp will both close when stack context is exited
        db = stack.enter_context(get_db_client())
        sftp = stack.enter_context(get_sftp_client())
        for episode in episodes:
            download_url = episode['downloadUrl']
            filename = make_filename(episode)
            remote_path = os.path.join(SFTP_PODCAST_FOLDER, filename)
            success = download_to_sftp_with_retries(
                download_url, remote_path, sftp)
            if not success:
                print(f"Skipping episode after max retries: {filename}")
                continue
            episode['audio_path'] = remote_path
            db.insert_episode(episode)


def download_episodes_and_save_locally(episodes):
    """This is typically only run on the PC hosting the SFTP server
        Take a list of episode dicts (with var names in RSS format,
        with audio_path and unique_id added
        NOT in DB format, which has some diff var names
        download the audio file, save it a local folder 
        if that succeeds, save metadata to db"""
    with get_db_client as db:
        for episode in episodes:
            download_url = episode['downloadUrl']
            filename = make_filename(episode)
            save_path = os.path.join(LOCAL_SAVE_FOLDER, filename)
            success = download_locally_with_retries(download_url, save_path)
            if not success:
                print(f"Skipping episode after max retries: {filename}")
                continue
            episode['audio_path'] = save_path
            db.insert_episode(episode)


def download_to_sftp_with_retries(url, dest_path, sftp, max_retries=6):
    consecutive_fails = 0
    while consecutive_fails <= max_retries:
        try:
            download_and_upload_episode(url, dest_path, sftp)
            return True
        except Exception as e:
            consecutive_fails = min(consecutive_fails + 1, max_retries)
            wait_time = 2 ** consecutive_fails
            print(
                f"Download failed (attempt {consecutive_fails}). Retrying in "
                f"{wait_time} sec:\n{e}"
            )
            time.sleep(wait_time)
    return False


def download_and_upload_episode(download_url, remote_path, sftp):
    with requests.get(download_url, stream=True) as r:
        r.raise_for_status()
        with sftp.sftp.file(remote_path, 'wb') as remote_file:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    remote_file.write(chunk)


def download_locally_with_retries(download_url, save_location, max_retries=6):
    consecutive_fails = 0
    while consecutive_fails <= max_retries:
        try:
            with requests.get(download_url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(save_location, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return True
        except Exception as e:
            consecutive_fails = min(consecutive_fails + 1, max_retries)
            wait_time = 2 ** consecutive_fails
            print(
                f"Download failed (attempt {consecutive_fails}). Retrying in "
                f"{wait_time} sec:\n{e}"
            )
            time.sleep(wait_time)
    return False


def make_filename(episode):
    # this filename is only referenced to get the extension. a custom filename will be used for the rest of the name
    filename = episode['download_url'].split('/')[-1]
    extension = filename.split('.')[-1]
    # remove any ? data that might be on the end of the url
    extension = extension.split('?')[0]
    return episode['unique_id'] + "." + extension
