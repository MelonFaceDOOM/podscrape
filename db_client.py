import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Optional, Tuple
from dotenv import load_dotenv
from contextlib import contextmanager
from sshtunnel import SSHTunnelForwarder


load_dotenv()

LEASE_MINUTES = 180  # how long eps are checked-out for transcription

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
                    id                TEXT PRIMARY KEY,
                    date_entered      TIMESTAMP DEFAULT current_timestamp,
                    audio_path        TEXT NOT NULL,      -- sftp://… or local path
                    guid              TEXT NOT NULL,
                    duration_s        NUMERIC,            -- optional, whole episode length
                    title             TEXT,
                    description       TEXT,
                    pub_date          TIMESTAMP,
                    download_url      TEXT,
                    podcast_id        INTEGER REFERENCES podcasts(id),
                    transcript_status TEXT NOT NULL DEFAULT 'pending',      -- 'pending' | 'processing' | 'done' | 'failed'
                    lease_expires_at  TIMESTAMPTZ,
                    worker_id         TEXT,
                    transcription_timestamp_completed TIMESTAMPTZ
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
            cur.execute(
                '''CREATE INDEX IF NOT EXISTS episodes_transcribe_queue_idx
                    ON episodes (transcript_status, lease_expires_at)''')
        self.conn.commit()

from datetime import datetime
from email.utils import parsedate_to_datetime

def insert_episode(self, episode_data):
    """
    Upsert an episode row.
    - Creates the podcast if needed.
    - Inserts a new episode with transcript_status='pending'.
    - On conflict, refreshes metadata fields but NEVER touches
      transcript_status / worker_id / lease_expires_at /
      transcription_timestamp_completed.
    """
    # Resolve podcast_id (create if missing)
    with self.conn.cursor() as cur:
        cur.execute('SELECT id FROM podcasts WHERE title = %s',
                    (episode_data['podcast_title'],))
        row = cur.fetchone()
        if row:
            podcast_id = row[0]
        else:
            cur.execute(
                'INSERT INTO podcasts (title) VALUES (%s) RETURNING id',
                (episode_data['podcast_title'],)
            )
            podcast_id = cur.fetchone()[0]

        # Parse pubDate robustly
        pub_dt = episode_data.get('pubDate')
        if isinstance(pub_dt, datetime):
            pub_date = pub_dt
        else:
            # Try RFC 2822 (common in RSS) first, then your original format
            try:
                pub_date = parsedate_to_datetime(pub_dt) if pub_dt else None
            except Exception:
                pub_date = datetime.strptime(
                    pub_dt, '%a, %d %b %Y %H:%M:%S %z'
                ) if pub_dt else None

        # Prepare fields
        ep_id       = episode_data['unique_id']
        guid        = episode_data.get('guid')
        title       = episode_data.get('title')
        download_url= episode_data.get('downloadUrl')
        audio_path  = episode_data.get('audio_path')   # may be None (download later)
        description = episode_data.get('description')

        # Insert (or update on conflict) – do not touch transcription state on updates
        cur.execute(
            """
            INSERT INTO episodes (
                id, guid, title, pub_date, download_url, audio_path,
                description, podcast_id,
                transcript_status, worker_id, lease_expires_at,
                transcription_timestamp_completed
            )
            VALUES (%s, %s, %s, %s, %s, %s,
                    %s, %s,
                    'pending', NULL, NULL,
                    NULL)
            ON CONFLICT (id) DO UPDATE SET
                guid         = COALESCE(EXCLUDED.guid, episodes.guid),
                title        = COALESCE(EXCLUDED.title, episodes.title),
                pub_date     = COALESCE(EXCLUDED.pub_date, episodes.pub_date),
                download_url = COALESCE(EXCLUDED.download_url, episodes.download_url),
                -- keep existing audio_path if we already have one; otherwise use the new one
                audio_path   = COALESCE(episodes.audio_path, EXCLUDED.audio_path),
                description  = COALESCE(EXCLUDED.description, episodes.description),
                podcast_id   = COALESCE(EXCLUDED.podcast_id, episodes.podcast_id)
            RETURNING id
            """,
            (ep_id, guid, title, pub_date, download_url, audio_path,
             description, podcast_id)
        )

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
        """
        Insert or update a word-level transcript for an episode.

        seg_rows:  [(start_s, end_s, text), ...]
        word_rows: [(seg_idx, word_idx, start_s, end_s, word), ...]
        """
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
        ON CONFLICT (seg_id, word_idx) DO NOTHING
        """

        # ---- Normalize to builtin types (no numpy scalars) ----
        norm_segs = [
            (int(idx), float(start), float(end), str(text))
            for idx, (start, end, text) in enumerate(seg_rows or [])
        ]

        norm_words = [
            (int(seg_idx), int(word_idx), float(w_start), float(w_end), str(token))
            for (seg_idx, word_idx, w_start, w_end, token) in (word_rows or [])
        ]

        # Group words by seg_idx so we can attach them after we know seg_id
        words_by_seg = defaultdict(list)
        for seg_idx, word_idx, start_w, end_w, word in norm_words:
            words_by_seg[seg_idx].append((word_idx, start_w, end_w, word))

        with self.conn.cursor() as cur:
            for seg_idx, start_s, end_s, text in norm_segs:
                # Insert/update segment row; get its PK
                cur.execute(seg_sql, (episode_id, seg_idx, start_s, end_s, text))
                seg_id = cur.fetchone()[0]

                # Batch insert this segment's words (if any)
                wrows = words_by_seg.get(seg_idx, [])
                if wrows:
                    rows = [(seg_id, widx, sw, ew, wtxt) for (widx, sw, ew, wtxt) in wrows]
                    execute_values(cur, word_sql, rows, page_size=500)

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
        
    def claim_episodes(self, worker_id: str, batch_size: int = 1):
        """
        claim eps to transcribe them so as to prevent other transcribers from overlapping jobs
        Atomically claim up to batch_size 'pending' (or expired) episodes for this worker.
        Uses SKIP LOCKED so concurrent workers don't collide.
        """
        now = datetime.now(timezone.utc)
        lease_until = now + timedelta(minutes=LEASE_MINUTES)

        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    WITH cte AS (
                      SELECT id
                        FROM episodes
                       WHERE transcript_status IN ('pending','processing')
                         AND (
                               transcript_status = 'pending'
                            OR lease_expires_at IS NULL
                            OR lease_expires_at < NOW()
                         )
                       ORDER BY date_entered DESC
                       FOR UPDATE SKIP LOCKED
                       LIMIT %s
                    )
                    UPDATE episodes e
                       SET transcript_status = 'processing',
                           worker_id = %s,
                           lease_expires_at = %s
                      FROM cte
                     WHERE e.id = cte.id
                 RETURNING e.id
                    """,
                    (batch_size, worker_id, lease_until),
                )
                rows = cur.fetchall()
                return [r[0] for r in rows]

    def mark_done(self, episode_id: str):
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE episodes
                       SET transcript_status = 'done',
                           worker_id = NULL,
                           lease_expires_at = NULL,
                           transcription_timestamp_completed = NOW()
                     WHERE id = %s
                """, (episode_id,))

    def mark_failed(self, episode_id: str, retry: bool = True):
        with self.conn:
            with self.conn.cursor() as cur:
                if retry:
                    cur.execute(
                        """
                        UPDATE episodes
                           SET transcript_status = 'pending',
                               worker_id = NULL,
                               lease_expires_at = NULL
                         WHERE id = %s
                        """,
                        (episode_id,),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE episodes
                           SET transcript_status = 'failed',
                               worker_id = NULL,
                               lease_expires_at = NULL
                         WHERE id = %s
                        """,
                        (episode_id,),
                    )

    def extend_lease(self, episode_id: str, minutes: int = 30):
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE episodes
                       SET lease_expires_at = NOW() + (%s || ' minutes')::interval
                     WHERE id = %s
                    """,
                    (minutes, episode_id),
                )
                
    def active_workers_info(self):
        """
        Return rows for workers currently processing (status='processing').
        Each row: (worker_id, processing_count, next_lease_exp, last_lease_exp)
        Also returns pending_total separately.
        """
        with self.conn.cursor() as cur:
            # workers with in-flight work
            cur.execute("""
                SELECT worker_id,
                       COUNT(*) AS processing,
                       MIN(lease_expires_at) AS next_lease_exp,
                       MAX(lease_expires_at) AS last_lease_exp
                  FROM episodes
                 WHERE transcript_status = 'processing'
              GROUP BY worker_id
              ORDER BY processing DESC NULLS LAST
            """)
            workers = cur.fetchall()

            # how many pending are waiting (not assigned to any worker)
            cur.execute("""
                SELECT COUNT(*)
                  FROM episodes
                 WHERE transcript_status = 'pending'
            """)
            pending_total = cur.fetchone()[0]

        # Normalize worker_id None -> "(none)"
        rows = []
        for w in workers:
            wid, processing, next_lease, last_lease = w
            rows.append({
                "worker_id": wid or "(none)",
                "processing": int(processing),
                "next_lease_exp": next_lease,
                "last_lease_exp": last_lease,
            })
        return {"pending_total": int(pending_total), "workers": rows}

    def nth_most_recent_transcription(self, n: int = 1) -> Optional[dict]:
        """
        Returns a dict with episode id, audio_path, and completion timestamp.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT id, audio_path, transcription_timestamp_completed
                  FROM episodes
                 WHERE transcription_timestamp_completed IS NOT NULL
                 ORDER BY transcription_timestamp_completed DESC, id DESC
                 OFFSET %s
                 LIMIT 1
            """, (max(0, n-1),))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "audio_path": row[1],
                "completed_at": row[2],
            }
            
    def transcript_stats(self, episode_id: str) -> dict:
        """
        duration_s: (max end_s - min start_s) over segments (0 if none)
        word_count: count(*) from transcript_words joined to this episode
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(MIN(start_s),0), COALESCE(MAX(end_s),0)
                  FROM transcript_segments
                 WHERE episode_id = %s
            """, (episode_id,))
            mn, mx = cur.fetchone() or (0.0, 0.0)
            duration_s = max(0.0, float(mx) - float(mn))

            cur.execute("""
                SELECT COUNT(*)
                  FROM transcript_words w
                  JOIN transcript_segments s ON s.id = w.seg_id
                 WHERE s.episode_id = %s
            """, (episode_id,))
            word_count = int(cur.fetchone()[0])

        return {"duration_s": duration_s, "word_count": word_count}
        
    def transcript_words_excerpt(self, episode_id: str, limit_words: int = 1000) -> Tuple[str, int]:
        """
        Returns (text, words_returned). Prefers word-level; falls back to segment text.
        """
        with self.conn.cursor() as cur:
            # Try word-level first
            cur.execute("""
                SELECT w.word
                  FROM transcript_words w
                  JOIN transcript_segments s ON s.id = w.seg_id
                 WHERE s.episode_id = %s
                 ORDER BY s.seg_idx, w.word_idx
                 LIMIT %s
            """, (episode_id, limit_words))
            words = [r[0] for r in cur.fetchall()]

            if words:
                text = " ".join(words)
                return text, len(words)

            # Fallback: segments text → split into words
            cur.execute("""
                SELECT text
                  FROM transcript_segments
                 WHERE episode_id = %s
                 ORDER BY seg_idx
            """, (episode_id,))
            segs = [r[0] or "" for r in cur.fetchall()]
            joined = " ".join(segs)
            toks = joined.split()
            excerpt = " ".join(toks[:limit_words])
            return excerpt, min(limit_words, len(toks))

    def recent_transcription_counts(self, limit_days: int = 7):
        """
        Return counts for the most recent N calendar dates where completions exist.
        Output: list of dicts [{day: date, count: int}, ...] ordered DESC by day.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT (transcription_timestamp_completed AT TIME ZONE 'UTC')::date AS day,
                       COUNT(*) AS cnt
                  FROM episodes
                 WHERE transcription_timestamp_completed IS NOT NULL
                   AND transcript_status = 'done'
              GROUP BY day
              ORDER BY day DESC
                 LIMIT %s
            """, (limit_days,))
            rows = cur.fetchall()
        return [{"day": r[0], "count": int(r[1])} for r in rows]
        
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    # TODO search transcript
