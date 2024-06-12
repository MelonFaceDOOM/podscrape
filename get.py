DL_DIRECTORY = "episodes/"
os.makedirs(DL_DIRECTORY, exist_ok=True)

def search_and_download():
    search_term = "vaccine"
    conn = psycopg2.connect(database=NETWORK_DB_CREDENTIALS['database'],
                        user=NETWORK_DB_CREDENTIALS['user'],
                        password=NETWORK_DB_CREDENTIALS['password'],
                        host=NETWORK_DB_CREDENTIALS['host'],
                        port=NETWORK_DB_CREDENTIALS['port'])
    episodes = search_title_and_description(conn, search_term)
    conn.close()
    transport = paramiko.Transport((SFTP_CREDENTIALS['host'], SFTP_CREDENTIALS['port']))
    transport.connect(username=SFTP_CREDENTIALS['username'], password=SFTP_CREDENTIALS['password'])
    sftp = paramiko.SFTPClient.from_transport(transport)
    episodes = [e for e in episodes if e['sftp_url'] is not None]
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
        
if __name__ == "__main__":
    main()
