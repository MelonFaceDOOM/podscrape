from datetime import datetime
import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from collections import defaultdict
from dotenv import load_dotenv
from contextlib import contextmanager
from sshtunnel import SSHTunnelForwarder


load_dotenv()


@contextmanager
def get_db_client():
    db_credential_map = {
        "database": "AZURE_DATABASE",
        "user": "AZURE_USER",
        "password": "AZURE_PASSWORD",
        "host": "AZURE_HOST",
        "port": "AZURE_PORT",
    }

    db_credentials = load_credentials_from_env(db_credential_map)
    db_credentials["port"] = int(db_credentials["port"])

    # Configure SSH Tunnel
    use_tunnel = os.getenv("USE_SSH_TUNNEL") == "1"
    tunnel = None
    if use_tunnel:
        tunnel = create_ssh_tunnel(
            db_credentials['host'], db_credentials['port'])
        db_credentials['host'] = 'localhost'
        db_credentials['port'] = tunnel.local_bind_port
    try:
        with DBClient(**db_credentials) as db:
            yield db
    finally:
        if tunnel:
            tunnel.stop()


def create_ssh_tunnel(host, port):
    ssh_tunnel_credential_map = {
        "ssh_host": "SSH_HOST",
        "ssh_username": "SSH_USERNAME",
        "ssh_pkey": "SSH_PKEY"
    }
    tunnel_credentials = load_credentials_from_env(ssh_tunnel_credential_map)
    tunnel_credentials["port"] = port
    tunnel = SSHTunnelForwarder(
        remote_bind_address=(host, port),
        local_bind_address=('localhost',),  # port will be dynamically assigned
        **tunnel_credentials
    )
    tunnel.start()
    return tunnel


def load_credentials_from_env(credential_map: dict) -> dict:
    loaded_credentials = dict()
    missing = []
    for k, v in credential_map.items():
        loaded = os.getenv(v)
        if loaded is None:
            missing.append(k)
        else:
            loaded_credentials[k] = loaded
    if missing:
        raise RuntimeError(
            f"Cannot load credentials: missing required environment variables: "
            f"{', '.join(missing)}"
        )
    return loaded_credentials


class DBClient:
    """
    usage should generally be:
        with get_db_client() as db:
            db.dostuff()
    this way conn auto-closes once the with-context is exited
    """

    def __init__(self, database, user, password, host, port):
        self.conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port
        )

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
            cur.execute(
                '''CREATE INDEX IF NOT EXISTS title_ts_idx ON episodes USING GIN (title_ts);''')
            cur.execute(
                '''CREATE INDEX IF NOT EXISTS description_ts_idx ON episodes USING GIN (description_ts);''')
            cur.execute(
                '''CREATE INDEX IF NOT EXISTS transcript_ts_idx ON episodes USING GIN (transcript_ts);''')
        self.conn.commit()

    def insert_episode(self, episode_data):
        with self.conn.cursor() as cur:
            cur.execute('SELECT id FROM podcasts WHERE title = %s',
                        (episode_data['podcast_title'],))
            podcast = cur.fetchone()
            if podcast:
                podcast_id = podcast[0]
            else:
                cur.execute('INSERT INTO podcasts (title) VALUES (%s) RETURNING id',
                            (episode_data['podcast_title'],))
                podcast_id = cur.fetchone()[0]
            pub_date = datetime.strptime(
                episode_data['pubDate'], '%a, %d %b %Y %H:%M:%S %z')
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
            cur.execute(
                '''SELECT * from episodes ORDER BY pub_date DESC NULLS LAST;''')
            r = cur.fetchall()
        return r

    def get_episodes_with_no_transcript(self):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT e.*, COUNT(s.id) AS segment_count
                FROM episodes e
                LEFT JOIN transcript_segments s ON e.id = s.episode_id
                GROUP BY e.id
                HAVING COUNT(s.id) = 0
                ORDER BY e.pub_date DESC NULLS LAST
            """)
            return cur.fetchall()

    def get_episodes_with_transcript(self):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT e.*, COUNT(s.id) AS segment_count
                FROM episodes e
                LEFT JOIN transcript_segments s ON e.id = s.episode_id
                GROUP BY e.id
                HAVING COUNT(s.id) > 0
                ORDER BY e.pub_date DESC NULLS LAST
            """)
            return cur.fetchall()

    def get_id_list(self):
        with self.conn.cursor() as cur:
            cur.execute('''SELECT id from episodes''')
            r = cur.fetchall()
            return [i[0] for i in r]

    def get_existing_ids(self, candidate_ids):
        """take a list of ids and return those that are actually in db"""
        with self.conn.cursor() as cur:
            query = "SELECT id FROM episodes WHERE id = ANY(%s)"
            cur.execute(query, (candidate_ids,))
            return {row[0] for row in cur.fetchall()}

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

    def search_title_and_description(self, search_term):
        vaccine_in_title = self.search_in_title(search_term)
        vaccine_in_description = self.search_in_description(search_term)

        vaccine_in_description_ids = [d['id'] for d in vaccine_in_description]
        vaccine_in_title_only = [
            d for d in vaccine_in_title if d['id'] not in vaccine_in_description_ids]
        both = vaccine_in_title_only + vaccine_in_description
        return both

    def search_in_title(self, search_string):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f'''SELECT *
                            FROM episodes
                            WHERE title_ts @@ plainto_tsquery('english', '{search_string}');''')
            results = cur.fetchall()
        return results

    def search_in_description(self, search_string):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f'''SELECT *
                            FROM episodes
                            WHERE description_ts @@ plainto_tsquery('english', '{search_string}');''')
            results = cur.fetchall()
        return results

    def word_level_insert(self, episode_id, seg_rows, word_rows):
        """Insert or update a word-level transcript for an episode."""
        seg_sql = """
        INSERT INTO transcript_segments
            (episode_id, seg_idx, start_s, end_s, text)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (episode_id, seg_idx) DO UPDATE
            SET start_s = EXCLUDED.start_s,
                end_s   = EXCLUDED.end_s,
                text    = EXCLUDED.text
        RETURNING id;
        """
        word_sql = """
        INSERT INTO transcript_words
            (seg_id, word_idx, start_s, end_s, word)
        VALUES %s
        ON CONFLICT DO NOTHING
        """

        # Pre-group word_rows by seg_idx for fast lookup
        word_map = defaultdict(list)
        for seg_idx, word_idx, start_w, end_w, word in word_rows:
            word_map[seg_idx].append((word_idx, start_w, end_w, word))

        with self.conn.cursor() as cur:
            for seg_idx, (start_s, end_s, text) in enumerate(seg_rows):
                # Insert/update segment and get seg_id
                cur.execute(seg_sql, (episode_id, seg_idx, start_s, end_s, text))
                seg_id = cur.fetchone()[0]

                # Prepare word rows for this segment
                words_for_segment = word_map.get(seg_idx, [])
                if words_for_segment:
                    word_rows_to_insert = [
                        (seg_id, word_idx, start_w, end_w, word)
                        for word_idx, start_w, end_w, word in words_for_segment
                    ]
                    # Batch insert words
                    execute_values(cur, word_sql, word_rows_to_insert)

            self.conn.commit()
        
    def get_transcript_for_episode_audio_path(self, audio_path):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT string_agg(ts.text, ' ' ORDER BY ts.seg_idx) AS full_transcript
                FROM transcript_segments ts
                JOIN episodes e ON ts.episode_id = e.id
                WHERE e.audio_path = %s;
            """, (audio_path,))
            result = cur.fetchone()
        return result[0] if result and result[0] else ""
        
    def get_transcript_for_episode(self, episode_id):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT string_agg(text, ' ' ORDER BY seg_idx) AS full_transcript
                FROM transcript_segments
                WHERE episode_id = %s;
            """, (episode_id,))
            result = cur.fetchone()
        return result[0] if result and result[0] else ""
        
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    # TODO search transcript
