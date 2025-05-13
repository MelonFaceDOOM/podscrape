"""
migrate_episodes.py – schema upgrade and data migration

* keeps these columns:
    id, date_entered, guid, title, description, pub_date,
    download_url, podcast_id
* renames old `sftp_url` → new `audio_path`
* sets duration_s = NULL  (you can back‑fill later)

If anything fails the transaction is rolled back.
"""
from db_client import get_client

sql1 = """

-- 1. new table under a temp name
CREATE TABLE IF NOT EXISTS episodes_new (
    id           TEXT PRIMARY KEY,
    date_entered TIMESTAMP DEFAULT current_timestamp,
    audio_path   TEXT NOT NULL,
    guid         TEXT NOT NULL,
    duration_s   NUMERIC,
    title        TEXT,
    description  TEXT,
    pub_date     TIMESTAMP,
    download_url TEXT,
    podcast_id   INTEGER REFERENCES podcasts(id)
);

-- 2. migrate rows (all at once with INSERT … SELECT)
INSERT INTO episodes_new (
    id, date_entered, audio_path, guid, duration_s,
    title, description, pub_date, download_url, podcast_id
)
SELECT  id,
        date_entered,
        COALESCE(sftp_url, '')   AS audio_path,
        guid,
        NULL                     AS duration_s,
        title,
        description,
        pub_date,
        download_url,
        podcast_id
FROM   episodes
ON CONFLICT (id) DO NOTHING;
"""

sqls = [
    """
        -- 3. drop old table & rename
        DROP TABLE episodes;
        ALTER TABLE episodes_new RENAME TO episodes;
    """,
    """
        -- 4. segment & word tables (empty now)
        CREATE TABLE transcript_segments (
            id          BIGSERIAL PRIMARY KEY,
            episode_id  TEXT REFERENCES episodes(id) ON DELETE CASCADE,
            seg_idx     INT,
            start_s     NUMERIC,
            end_s       NUMERIC,
            text        TEXT,
            UNIQUE (episode_id, seg_idx)
        );
    """,
    """
        CREATE TABLE transcript_words (
            seg_id    BIGINT REFERENCES transcript_segments(id) ON DELETE CASCADE,
            word_idx  INT,
            start_s   NUMERIC,
            end_s     NUMERIC,
            word      TEXT,
            PRIMARY KEY (seg_id, word_idx)
        );
    """,
    """
    -- 5. indexes
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
    """
]

def main():
    client = get_client()
    with client.conn.cursor() as cur:
        for sql in sqls:
            cur.execute(sql)
            print('finished a thing')
        # cur.execute("""select count(*) from episodes""")
        # r = cur.fetchone()
        # print("eps before:", r)
        # cur.execute("""select count(*) from episodes_new""")
        # r = cur.fetchone()
        # print("eps after:", r)
        client.conn.commit()
    # print("✅ migration complete – new schema in place.")

if __name__ == "__main__":
    main()