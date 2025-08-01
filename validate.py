from rss import get_unscraped_episodes
from sftp_client import get_sftp_client
from db_client import get_db_client


"""
why are some db items not in sftp?
i know some downloads fail, but then why were they entered in db at all?
try block should have exited

how many from json aren't in db?

what's the 1 that is in sftp but not db?
"""


def compare():
    unscraped_episodes = get_unscraped_episodes()
    db_not_in_sftp, sftp_not_in_db = compare_sftp_to_db()
    with open('db.txt', 'w') as f:
        f.write('\n'.join(db_not_in_sftp))
    with open('sftp.txt', 'w') as f:
        f.write('\n'.join(sftp_not_in_db))
    rss_urls = [e['downloadUrl'] for e in unscraped_episodes]
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
            sftp_url = "podcasts//" + \
                make_filename(
                    {"downloadUrl": ep['download_url'], "unique_id": ep['id']})
            sftp_urls.append((sftp_url, ep['id']))
        for sftp_url, ep_id in sftp_urls:
            with db.conn.cursor() as cur:
                cur.execute(
                    '''UPDATE episodes SET sftp_url = %s WHERE id = %s''', (sftp_url, ep_id))
            db.conn.commit()


if __name__ == "__main__":
    main()
