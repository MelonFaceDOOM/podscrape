from db_client import get_db_client

with get_db_client() as db:
    with db.conn.cursor() as cur:
        # Add columns 
        cur.execute("""
            ALTER TABLE episodes
              ADD COLUMN IF NOT EXISTS transcript_status text NOT NULL DEFAULT 'pending',
              ADD COLUMN IF NOT EXISTS lease_expires_at timestamptz,
              ADD COLUMN IF NOT EXISTS worker_id text
        """)
    db.conn.commit()

    # Backfill: mark DONE where transcript already exists
    with db.conn.cursor() as cur:
        cur.execute("""
            UPDATE episodes e
               SET transcript_status = 'done',
                   worker_id = NULL,
                   lease_expires_at = NULL
             WHERE transcript_status <> 'done'
               AND EXISTS (
                   SELECT 1 FROM transcript_segments ts
                    WHERE ts.episode_id = e.id
               )
        """)
        #  normalize any NULLs to 'pending'
        cur.execute("""
            UPDATE episodes
               SET transcript_status = 'pending'
             WHERE transcript_status IS NULL
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS episodes_transcribe_queue_idx
              ON episodes (transcript_status, lease_expires_at)
        """)
    db.conn.commit()

print("Migration complete.")