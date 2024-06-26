from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from config import NETWORK_DB_CREDENTIALS

def get_client():
    return Client(database=NETWORK_DB_CREDENTIALS['database'],
                    user=NETWORK_DB_CREDENTIALS['user'],
                    password=NETWORK_DB_CREDENTIALS['password'],
                    host=NETWORK_DB_CREDENTIALS['host'],
                    port=NETWORK_DB_CREDENTIALS['port'])
    

class Client:
    def __init__(self, database, user, password, host, port):
        self.conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        
    def close(self):
        self.conn.close()
        
    def make_core_tables(self):
        with self.conn.cursor() as cur:
            cur.execute('''CREATE TABLE IF NOT EXISTS podcasts
                             (id SERIAL PRIMARY KEY,
                             date_entered TIMESTAMP DEFAULT current_timestamp,
                             title TEXT NOT NULL,
                             rss_url TEXT)''')
            cur.execute("""CREATE TABLE IF NOT EXISTS episodes
                             (id TEXT PRIMARY KEY NOT NULL,
                             date_entered TIMESTAMP DEFAULT current_timestamp,
                             guid TEXT NOT NULL,
                             title TEXT NOT NULL,
                             description TEXT,
                             pub_date TIMESTAMP,
                             download_url TEXT,
                             sftp_url TEXT,
                             transcript TEXT,
                             podcast_id INTEGER,
                             title_ts TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', replace(title, '''',''))) STORED,
                             description_ts TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', replace(description, '''',''))) STORED,
                             transcript_ts TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', replace(transcript, '''',''))) STORED,
                             FOREIGN KEY (podcast_id) REFERENCES podcasts (id))""")
            cur.execute('''CREATE INDEX IF NOT EXISTS title_ts_idx ON episodes USING GIN (title_ts);''')
            cur.execute('''CREATE INDEX IF NOT EXISTS description_ts_idx ON episodes USING GIN (description_ts);''')
            cur.execute('''CREATE INDEX IF NOT EXISTS transcript_ts_idx ON episodes USING GIN (transcript_ts);''')
            self.conn.commit()
    
    # def truncate_tables(self):
        # with self.conn.cursor() as cur:
            # try:
                # cur.execute('''TRUNCATE TABLE podcasts, episodes RESTART IDENTITY CASCADE;''')
                # self.conn.commit()
                # print("Tables truncated successfully.")
            # except Exception as e:
                # print(f"An error occurred: {e}")
                
    # def drop_tables(self):
        # with self.conn.cursor() as cur:
            # try:
                # cur.execute('''DROP TABLE IF EXISTS episodes CASCADE;''')
                # cur.execute('''DROP TABLE IF EXISTS podcasts CASCADE;''')
                # self.conn.commit()
                # print("Tables dropped successfully.")
            # except Exception as e:
                # print(f"An error occurred: {e}")
            
    def bulk_insert(self, rows, cols, table_name):
        # rows and cols are both lists of strings. rows must be ordered in the same order as cols
        cur = self.conn.cursor()
        cols_string = ", ".join(cols)
        query = f"INSERT INTO {table_name} ({cols_string}) VALUES %s ON CONFLICT DO NOTHING"
        execute_values(cur, query, rows)
        self.conn.commit()
        cur.close()
    
    def insert_episode(self, episode_data):
        with self.conn.cursor() as cur:
            cur.execute('SELECT id FROM podcasts WHERE title = %s', (episode_data['podcast_title'],))
            podcast = cur.fetchone()
            if podcast:
                podcast_id = podcast[0]
            else:
                cur.execute('INSERT INTO podcasts (title) VALUES (%s) RETURNING id', (episode_data['podcast_title'],))
                podcast_id = cur.fetchone()[0]    
            pub_date = datetime.strptime(episode_data['pubDate'], '%a, %d %b %Y %H:%M:%S %z')
            cur.execute('''
                INSERT INTO episodes (id, guid, title, pub_date, download_url, sftp_url, description, transcript, podcast_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)''', 
                 (episode_data['unique_id'],
                  episode_data['guid'],
                  episode_data['title'],
                  pub_date,
                  episode_data['downloadUrl'],
                  episode_data['sftp_url'],
                  episode_data.get('description', None),
                  episode_data.get('transcript', None),
                  podcast_id))
            self.conn.commit()
            
    def get_podcasts(self):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute('''SELECT * from podcasts''')
            r = cur.fetchall()
        return r
            
    def get_id_list(self):
        with self.conn.cursor() as cur:
            cur.execute('''SELECT id from episodes''')
            r = cur.fetchall()
            return [i[0] for i in r]
            
    def ep_count(self):
        with self.conn.cursor() as cur:
            cur.execute('''SELECT COUNT(*) from episodes''')
            r = cur.fetchone()
            return r[0]
            
            
    def recent_episode_counts(self):
        """get count for episodes saved to db in each of the last 7 days"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    DATE(date_entered) AS day, 
                    COUNT(*) AS episode_count
                FROM episodes
                WHERE date_entered >= NOW() - INTERVAL '7 days'
                GROUP BY day
                ORDER BY day
            """)

            # Fetch and print the results
            rows = cur.fetchall()
            return rows
