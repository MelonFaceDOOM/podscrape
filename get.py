import os 
import psycopg2
from psycopg2.extras import RealDictCursor
import paramiko
from config import NETWORK_DB_CREDENTIALS, SFTP_CREDENTIALS

search_term = "vaccine"
DL_DIRECTORY = "episodes/"
os.makedirs(DL_DIRECTORY, exist_ok=True)

def search_and_download():
    transport = paramiko.Transport((SFTP_CREDENTIALS['host'], SFTP_CREDENTIALS['port']))
    transport.connect(username=SFTP_CREDENTIALS['username'], password=SFTP_CREDENTIALS['password'])
    sftp = paramiko.SFTPClient.from_transport(transport)
    
    conn = psycopg2.connect(database=NETWORK_DB_CREDENTIALS['database'],
                        user=NETWORK_DB_CREDENTIALS['user'],
                        password=NETWORK_DB_CREDENTIALS['password'],
                        host=NETWORK_DB_CREDENTIALS['host'],
                        port=NETWORK_DB_CREDENTIALS['port'])
    episodes = search_title_and_description(conn, search_term)
    conn.close()
            
    if len(episodes) == 0:
        print(f"no episodes found with {search_term} in title or description.")
        return
    print(f"{len(episodes)} episodes found with {search_term} in title or description.")
    
    no_sftp_url = [e for e in episodes if e['sftp_url'] is None]
    if len(no_sftp_url) > 0:
        print(f"{len(no_sftp_url)} episodes did not have a sftp_url value and cannot be downloaded")
    episodes = [e for e in episodes if e['sftp_url'] is not None]
    
    sftp_urls = get_sftp_urls(sftp)   
    sftp_url_not_found = [e for e in episodes if e['sftp_url'] not in sftp_urls]
    if len(sftp_url_not_found) > 0:
        print(f"{len(no_sftp_url)} episodes couldn't be found in the sftp filelist.") 
    
    sftp_found = [e for e in episodes if e['sftp_url'] in sftp_urls]
    if len(sftp_found) == 0 :
        print("Nothing to download, exiting.")
        return
        
    print(f"Downloading {len(episodes)} episodes...")
    download_episodes(sftp, episodes)
    sftp.close()
    transport.close()

def download_episodes(sftp, episodes):
    for episode in episodes:
        filename = os.path.basename(episode['sftp_url'])
        local_path = os.path.join(DL_DIRECTORY, filename)
        sftp.get(episode['sftp_url'], local_path)

def search_title_and_description(conn, search_term):
    vaccine_in_title = search_in_title(conn, search_term)
    vaccine_in_description = search_in_description(conn, search_term)

    vaccine_in_description_ids = [d['id'] for d in vaccine_in_description]
    vaccine_in_title_only = [d for d in vaccine_in_title if d['id'] not in vaccine_in_description_ids]
    both = vaccine_in_title_only + vaccine_in_description
    return both
    
def search_in_title(conn, search_string):
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(f'''SELECT *
                        FROM episodes
                        WHERE title_ts @@ plainto_tsquery('english', '{search_string}');''')
        results = cur.fetchall()
        cur.close()
        return results
        
def search_in_description(conn, search_string):
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(f'''SELECT *
                        FROM episodes
                        WHERE description_ts @@ plainto_tsquery('english', '{search_string}');''')
        results = cur.fetchall()
        cur.close()
        return results
        
def get_sftp_urls(sftp):
    sftp_urls = sftp.listdir_attr('podcasts/')
    sftp_urls = ["podcasts/" + attr.filename for attr in sftp_urls]
    return sftp_urls


if __name__ == "__main__":
    search_and_download()
