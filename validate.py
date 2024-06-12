import os
import psycopg2
import paramiko
from datetime import datetime
from psycopg2.extras import RealDictCursor
import pandas as pd
from config import NETWORK_DB_CREDENTIALS, SFTP_CREDENTIALS
from rss import get_unscraped_episodes


"""
why are some db items not in sftp?
i know some downloads fail, but then why were they entered in db at all?
try block should have exited

how many from json aren't in db?

what's the 1 that is in sftp but not db?
"""

def main():
    compare()
    
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
    conn = psycopg2.connect(database=NETWORK_DB_CREDENTIALS['database'],
                        user=NETWORK_DB_CREDENTIALS['user'],
                        password=NETWORK_DB_CREDENTIALS['password'],
                        host=NETWORK_DB_CREDENTIALS['host'],
                        port=NETWORK_DB_CREDENTIALS['port'])
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('''SELECT * FROM episodes''')
        db_episodes = cur.fetchall()
    conn.close()
    db_urls = [e['sftp_url'] for e in db_episodes]
    transport = paramiko.Transport((SFTP_CREDENTIALS['host'], SFTP_CREDENTIALS['port']))
    transport.connect(username=SFTP_CREDENTIALS['username'], password=SFTP_CREDENTIALS['password'])
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp_urls = sftp.listdir_attr('podcasts/')
    sftp_urls = ["podcasts/" + attr.filename for attr in sftp_urls]
    sftp_not_in_db = []
    for sftp_url in sftp_urls:
        if sftp_url and sftp_url not in db_urls:
            sftp_not_in_db.append(sftp_url)
    print(f"removing {len(sftp_not_in_db)} files from sftp. hopefully u doublechecked before deleting...")
    for url in sftp_not_in_db:
        # sftp.remove(url)
    sftp.close()
    transport.close()
    
def compare_sftp_to_db():
    conn = psycopg2.connect(database=NETWORK_DB_CREDENTIALS['database'],
                        user=NETWORK_DB_CREDENTIALS['user'],
                        password=NETWORK_DB_CREDENTIALS['password'],
                        host=NETWORK_DB_CREDENTIALS['host'],
                        port=NETWORK_DB_CREDENTIALS['port'])
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute('''SELECT * FROM episodes''')
        db_episodes = cur.fetchall()
    conn.close()
    db_urls = [e['sftp_url'] for e in db_episodes]
    transport = paramiko.Transport((SFTP_CREDENTIALS['host'], SFTP_CREDENTIALS['port']))
    transport.connect(username=SFTP_CREDENTIALS['username'], password=SFTP_CREDENTIALS['password'])
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp_urls = sftp.listdir_attr('podcasts/')
    sftp_urls = ["podcasts/" + attr.filename for attr in sftp_urls]
    db_not_in_sftp = []
    sftp_not_in_db = []
    for db_url in db_urls:
        if db_url and db_url not in sftp_urls:
            db_not_in_sftp.append(db_url)
    for sftp_url in sftp_urls:
        if sftp_url and sftp_url not in db_urls:
            sftp_not_in_db.append(sftp_url)
    sftp.close()
    transport.close()
    return db_not_in_sftp, sftp_not_in_db
    
    
def fix_db():
    """add sftp_url to db"""
    conn = psycopg2.connect(database=NETWORK_DB_CREDENTIALS['database'],
                        user=NETWORK_DB_CREDENTIALS['user'],
                        password=NETWORK_DB_CREDENTIALS['password'],
                        host=NETWORK_DB_CREDENTIALS['host'],
                        port=NETWORK_DB_CREDENTIALS['port'])

    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute('''select id, download_url, sftp_url from episodes''')
    eps = cur.fetchall()
    eps = [e for e in eps if e['sftp_url'] is None]
    ftp_urls = []
    
    for ep in eps:
        ftp_url = "podcasts//" + make_filename({"downloadUrl": ep['download_url'], "unique_id": ep['id']})
        ftp_urls.append((ftp_url, ep['id']))
    # cur.execute('''ALTER TABLE episodes ADD COLUMN sftp_url TEXT''')
    for ftp_url, ep_id in ftp_urls:
        cur.execute('''UPDATE episodes SET sftp_url = %s WHERE id = %s''', (ftp_url, ep_id))
    conn.commit()
    

if __name__ == "__main__":
    main()
