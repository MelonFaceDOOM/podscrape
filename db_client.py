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
            cur.execute("""
                CREATE TABLE IF NOT EXISTS podcasts (
                    id           SERIAL PRIMARY KEY,
                    date_entered TIMESTAMP DEFAULT current_timestamp,
                    title        TEXT NOT NULL,
                    rss_url      TEXT
                    );
             """)
            cur.execute("""
                CREATE TABLE episodes (
                    id             TEXT PRIMARY KEY,
                    date_entered   TIMESTAMP DEFAULT current_timestamp,
                    audio_path     TEXT NOT NULL,      -- sftp://… or local path
                    guid           TEXT NOT NULL,
                    duration_s     NUMERIC,            -- optional, whole episode length
                    title          TEXT,
                    description    TEXT,
                    pub_date       TIMESTAMP,
                    download_url   TEXT,
                    podcast_id     INTEGER REFERENCES podcasts(id)
                );
            """)
            cur.execute("""
                CREATE TABLE transcript_segments (
                    id          BIGSERIAL PRIMARY KEY,
                    episode_id  TEXT REFERENCES episodes(id) ON DELETE CASCADE,
                    seg_idx     INT,                -- 0,1,2…
                    start_s     NUMERIC,            -- 12.34
                    end_s       NUMERIC,            -- 18.92
                    text        TEXT,
                    UNIQUE(episode_id, seg_idx)
                );
            """)
            cur.execute("""
                CREATE TABLE transcript_words (
                    seg_id      BIGINT REFERENCES transcript_segments(id) ON DELETE CASCADE,
                    word_idx    INT,                -- position inside segment
                    start_s     NUMERIC,
                    end_s       NUMERIC,
                    word        TEXT,
                    PRIMARY KEY (seg_id, word_idx)
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS seg_time_idx
                    ON transcript_segments (episode_id, start_s);
            
                CREATE INDEX IF NOT EXISTS seg_text_gin
                    ON transcript_segments
                    USING GIN (to_tsvector('english', replace(text, '''', '')));
                
                CREATE INDEX IF NOT EXISTS ep_title_gin
                    ON episodes
                    USING GIN (to_tsvector('english', replace(title, '''', '')));
                
                CREATE INDEX IF NOT EXISTS ep_desc_gin
                    ON episodes
                    USING GIN (to_tsvector('english', replace(description, '''', '')));
            """)


    def make_core_tables(self):
        with self.conn.cursor() as cur:
            cur.execute('''CREATE INDEX IF NOT EXISTS title_ts_idx ON episodes USING GIN (title_ts);''')
            cur.execute('''CREATE INDEX IF NOT EXISTS description_ts_idx ON episodes USING GIN (description_ts);''')
            cur.execute('''CREATE INDEX IF NOT EXISTS transcript_ts_idx ON episodes USING GIN (transcript_ts);''')
            self.conn.commit()

    
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
                INSERT INTO episodes (id, guid, title, pub_date, download_url, audio_path, description, podcast_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''',
                 (episode_data['unique_id'],
                  episode_data['guid'],
                  episode_data['title'],
                  pub_date,
                  episode_data['downloadUrl'],
                  episode_data['audio_path'],
                  episode_data.get('description', None),
                  podcast_id))
            self.conn.commit()
            
    def get_podcasts(self):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute('''SELECT * from podcasts''')
            r = cur.fetchall()
        return r

    def get_episodes(self):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute('''SELECT * from episodes ORDER BY pub_date DESC NULLS LAST;''')
            r = cur.fetchall()
        return r

    def get_episodes_with_no_transcript(self):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute('''SELECT * from episodes ORDER BY pub_date DESC NULLS LAST;''')
            episodes = cur.fetchall()
            episodes_with_no_transcript = []
            for episode in episodes:
                cur.execute(
                '''
                        SELECT COUNT(*)
                        FROM transcript_words
                        INNER JOIN transcript_segments ON
                        transcript_words.seg_id = transcript_segments.id
                        WHERE transcript_segments.episode_id = %s
                    ''',
                    (episode['id'],)
                )
                r = cur.fetchone()
                transcript_word_count = r['count']
                if transcript_word_count < 10:
                    episodes_with_no_transcript.append(episode)
            return episodes_with_no_transcript
            
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
        """
        Get episode counts for the 7 most recent *distinct* days
        that have at least one episode in the database.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT day, COUNT(*) AS episode_count
                FROM (
                    SELECT DATE(date_entered) AS day
                    FROM episodes
                    GROUP BY day
                    ORDER BY day DESC
                    LIMIT 7
                ) recent_days
                JOIN episodes ON DATE(episodes.date_entered) = recent_days.day
                GROUP BY day
                ORDER BY day DESC
            """)
            return cur.fetchall()
