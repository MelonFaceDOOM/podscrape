from rss import get_unscraped_episodes
from sftp_client import get_sftp_client
from db_client import get_db_client, load_credentials_from_env, DBClient
import os
from contextlib import ExitStack
from collections import Counter
from scrape import make_filename, download_to_sftp_with_retries
from dotenv import load_dotenv


"""
Purpose:
The podscrape data is currently split between 3 locations:
- all audio files on SFTP
- all podcast metadata on network db
- some podcast metadata on azure (from initial transfer when it was set up)

Recurring scraping still saves to network db.

This script checks mostly checks the overlap between the three sources
while also doing a few basic data validity checks


Conclusions:
2157 eps in sftp but not in db/azure
db has more than azure. simply update azure with db
  (without overwriting transcript on existing rows) later on

REGARDING THE 2157 MISSING EPISODES
what to do about the 2157 in sftp...? Could delete. 
In RSS but not in DB should catch anything worth re-scraping

On the other hand...
It's possible that a podcast deleted its RSS, or maybe
many podcasts deleted some eps. Do I really want to purge my
db when this happens?
Maybe I should look at the 2157 more carefully. 

In any case, not urgent
"""

load_dotenv()
SFTP_PODCAST_FOLDER = os.getenv("SFTP_PODCAST_FOLDER")


def validate_data():
    db_credential_map = {
        "database": "DB_DATABASE",
        "user": "DB_USER",
        "password": "DB_PASSWORD",
        "host": "DB_HOST",
        "port": "DB_PORT",
    }
    db_credentials = load_credentials_from_env(db_credential_map)
    db_credentials["port"] = int(db_credentials["port"])
    with ExitStack() as stack:
        # 1) Load db & sftp eps
        db = stack.enter_context(DBClient(**db_credentials))
        sftp = stack.enter_context(get_sftp_client())
        azure = stack.enter_context(get_db_client())

        db_episodes = db.get_episodes()
        sftp_attrs = sftp.sftp.listdir_attr('podcasts/')
        sftp_episodes = ["podcasts/" + attr.filename for attr in sftp_attrs]
        azure_episodes = azure.get_episodes()

        # 2) Check for blank guid
        check_for_blank_guid(db_episodes)
        check_for_blank_guid(azure_episodes)

        # 3) Check url validity
        check_urls([ep['audio_path'] for ep in db_episodes])
        check_urls(sftp_episodes)
        check_urls([ep['audio_path'] for ep in azure_episodes])

        # 4) Check for overlap between 2 sets
        db_extra_episodes, sftp_extra_files = check_db_sftp_overlap(
            db_episodes, sftp_episodes)

        # 5) Where there are eps in db but not sftp, try to download to sftp
        if db_extra_episodes:
            download_missing(db, sftp, db_extra_episodes)

            # 5B) check how it went (func will print status)
            db_episodes = db.get_episodes()
            sftp_attrs = sftp.sftp.listdir_attr('podcasts/')
            sftp_episodes = ["podcasts/" +
                             attr.filename for attr in sftp_attrs]
            db_extra_episodes, sftp_extra_files = check_db_sftp_overlap(
                db_episodes, sftp_episodes)

        # 6) Azure
        azure_extra_episodes, db_extra_episodes = check_azure_db_overlap(
            azure_episodes, db_episodes)

        # 7) Of those in Azure but not in DB, how many are in sftp_extra?
        azure_extra_in_sftp_extra = []
        azure_extra_not_in_sftp_extra = []
        if azure_extra_episodes:
            for episode in azure_extra_episodes:
                if episode['audio_path'] in sftp_extra_files:
                    azure_extra_in_sftp_extra.append(episode)
                else:
                    azure_extra_not_in_sftp_extra.append(episode)
        if azure_extra_in_sftp_extra:
            print(f"There are {len(azure_extra_in_sftp_extra)
                               } eps in Azure that aren't in DB but that are also in sftp extra")
        if azure_extra_not_in_sftp_extra:
            print(f"Big mystery: There are {len(
                azure_extra_not_in_sftp_extra)} eps in Azure that aren't in the extra sftp episodes")


def check_db_sftp_overlap(db_episodes, sftp_episodes):
    """ Returns:
     - list of episodes (dicts) found in db but not in sftp
     - list of urls (strs) found in sftp but not db """
    db_url_map = {e['audio_path']: e for e in db_episodes}
    db_urls = list(db_url_map.keys())

    db_set = set(db_urls)
    sftp_set = set(sftp_episodes)

    db_extra_urls = db_set - sftp_set
    sftp_extra_urls = sftp_set - db_set

    db_extra_episodes = [db_url_map[url] for url in db_extra_urls]
    sftp_extra_files = [url for url in sftp_extra_urls]

    if db_extra_episodes:
        print(f"{len(db_extra_episodes)} extra episodes found in the database")
    if sftp_extra_files:
        print(f"{len(sftp_extra_files)} extra episodes found in SFTP server")

    return db_extra_episodes, sftp_extra_files


def check_azure_db_overlap(azure_episodes, db_episodes):
    """ Returns:
     - list of episodes (dicts) found in azure but not in db 
     - list of episodes (dicts) found in db but not in azure 
     """
    azure_url_map = {e['audio_path']: e for e in azure_episodes}
    azure_urls = list(azure_url_map.keys())

    db_url_map = {e['audio_path']: e for e in db_episodes}
    db_urls = list(db_url_map.keys())

    azure_set = set(azure_urls)
    db_set = set(db_urls)

    azure_extra_urls = azure_set - db_set
    db_extra_urls = db_set - azure_set

    azure_extra_episodes = [azure_url_map[url] for url in azure_extra_urls]
    db_extra_episodes = [db_url_map[url] for url in db_extra_urls]

    if azure_extra_episodes:
        print(f"{len(azure_extra_episodes)
                 } extra episodes found in azure but not db")
    if db_extra_episodes:
        print(f"{len(db_extra_episodes)
                 } extra episodes found in the database but not in azure")
    if not azure_extra_episodes and not db_extra_episodes:
        print("perfect overlap between azure and db")

    return azure_extra_episodes, db_extra_episodes


def check_for_blank_guid(episodes):
    eps_with_no_guid = []
    for ep in episodes:
        if str(ep['guid']).strip() == "":
            eps_with_no_guid.append(ep)
    if len(eps_with_no_guid) == 0:
        print("All episodes have a guid")
    else:
        print(f"{len(eps_with_no_guid)} episodes have no guid")


def download_missing(db, sftp, episodes):
    # given a list of episodes, download to sftp
    # dont update db because presumably this should already be there
    for episode in episodes:
        download_url = episode['download_url']
        # make_filename is intended to run on rss data,
        # which will have a unqiue_id (which becomes id in db)
        # so unique_id is added now
        episode['unique_id'] = episode['id']
        filename = make_filename(episode)
        remote_path = os.path.join(SFTP_PODCAST_FOLDER, filename)
        success = download_to_sftp_with_retries(
            download_url, remote_path, sftp)
        if not success:
            print(f"Skipping episode after max retries: {filename}")
        else:
            print(f"ep dl'd to sftp: {filename}")


def check_urls(filepaths):
    invalid_dirs = []
    for path in filepaths:
        directory, filename = os.path.split(path)
        name, ext = os.path.splitext(filename)
        if directory != "podcasts":
            invalid_dirs.append(directory)
    if not invalid_dirs:
        print("All file dirs are in expected format")
    else:
        print("Invalid directories found:")
        dir_counts = Counter(invalid_dirs)
        for dir_val, count in dir_counts.items():
            print(f"  {dir_val or '(root)'}: {count}")


def compare():
    unscraped_episodes = get_unscraped_episodes()
    db_not_in_sftp, sftp_not_in_db = compare_sftp_to_db()
    with open('db.txt', 'w') as f:
        f.write('\n'.join(db_not_in_sftp))
    with open('sftp.txt', 'w') as f:
        f.write('\n'.join(sftp_not_in_db))
    rss_urls = [e['download_url'] for e in unscraped_episodes]
    with open('rss.txt', 'w') as f:
        f.write('\n'.join(rss_urls))
    print(f"{len(unscraped_episodes)} in RSS file that are not in DB.")
    print(f"{len(db_not_in_sftp)} in DB that are not in SFTP.")
    print(f"{len(sftp_not_in_db)} in SFTP that are not in DB.")


def remove_sftp_duds():
    with get_db_client() as db:
        db_episodes = db.get_episodes()
    db_urls = [e['sftp_url'] for e in db_episodes]
    with get_sftp_client() as sftp:
        sftp_urls = sftp.sftp.listdir_attr('podcasts/')
        sftp_urls = ["podcasts/" + attr.filename for attr in sftp_urls]
        sftp_not_in_db = []
        for sftp_url in sftp_urls:
            if sftp_url and sftp_url not in db_urls:
                sftp_not_in_db.append(sftp_url)
        print(
            f"removing {len(sftp_not_in_db)} files from sftp."
            f"hopefully u doublechecked before deleting..."
        )
        # for url in sftp_not_in_db:
        # sftp.sftp.remove(url)


def compare_sftp_to_db():
    with get_db_client() as db:
        db_episodes = db.get_episodes()
    db_urls = [e['sftp_url'] for e in db_episodes]
    with get_sftp_client() as sftp:
        sftp_urls = sftp.sftp.listdir_attr('podcasts/')
        sftp_urls = ["podcasts/" + attr.filename for attr in sftp_urls]
        db_not_in_sftp = []
        sftp_not_in_db = []
        for db_url in db_urls:
            if db_url and db_url not in sftp_urls:
                db_not_in_sftp.append(db_url)
        for sftp_url in sftp_urls:
            if sftp_url and sftp_url not in db_urls:
                sftp_not_in_db.append(sftp_url)
    return db_not_in_sftp, sftp_not_in_db


def fix_db():
    """add sftp_url to db"""
    with get_db_client() as db:
        episodes = db.get_episodes()

        episodes = [e for e in episodes if e['sftp_url'] is None]
        sftp_urls = []

        for ep in episodes:
            sftp_url = "podcasts//"
            make_filename(
                {"download_url": ep['download_url'], "guid": ep['id']})
            sftp_urls.append((sftp_url, ep['id']))
        for sftp_url, ep_id in sftp_urls:
            with db.conn.cursor() as cur:
                cur.execute(
                    '''UPDATE episodes SET sftp_url = %s WHERE id = %s''', (sftp_url, ep_id))
            db.conn.commit()


if __name__ == "__main__":
    validate_data()
