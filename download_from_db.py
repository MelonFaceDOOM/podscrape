import os
from db_client import get_db_client
from sftp_client import get_sftp_client

SEARCH_TERM = "vaccine"
DL_DIRECTORY = "episodes/"
SFTP_PODCAST_FOLDER = os.getenv("SFTP_PODCAST_FOLDER")
os.makedirs(DL_DIRECTORY, exist_ok=True)


def search_and_download(search_term=SEARCH_TERM):
    # todo what about metadata? should i save a csv with data on db row data?
    # maybe save 1 row per episode. include transcript? maybe flat transcript, def not individual rows.
    # but might need to warn user about how many eps and size estimate before proceeding to actually save.
    with get_db_client() as db:
        episodes = db.search_title_and_description(search_term)

    if len(episodes) == 0:
        print(f"no episodes found with {search_term} in title or description.")
        return
    print(
        f"{len(episodes)} episodes found with "
        f"{search_term} in title or description."
    )

    no_sftp_url = [e for e in episodes if e['sftp_url'] is None]
    if len(no_sftp_url) > 0:
        print(
            f"{len(no_sftp_url)} "
            f"episodes did not have a sftp_url value and cannot be downloaded"
        )
    episodes = [e for e in episodes if e['sftp_url'] is not None]
    expected_urls = [e['sftp_url'] for e in episodes]

    with get_sftp_client() as sftp:
        found, missing = sftp.locate_files(expected_urls, SFTP_PODCAST_FOLDER)
        if len(missing) > 0:
            print(f"{len(missing)} episodes couldn't be found in the sftp filelist.")

        if len(found) == 0:
            print("Nothing to download, exiting.")
            return

        print(f"Downloading {len(found)} episodes...")
        download_episodes(sftp, found, DL_DIRECTORY)


def download_episodes(sftp, episodes, save_folder):
    for episode in episodes:
        filename = os.path.basename(episode['sftp_url'])
        local_path = os.path.join(save_folder, filename)
        sftp.sftp.get(episode["sftp_url"], local_path)
