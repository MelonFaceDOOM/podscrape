from db_client import get_db_client

SQL = """
BEGIN;

-- Add completion timestamp if missing
ALTER TABLE episodes
  ADD COLUMN IF NOT EXISTS transcription_timestamp_completed timestamptz;

-- Mark episodes as 'done' if they already have transcript segments.
UPDATE episodes e
   SET transcript_status = 'done'
 WHERE transcript_status <> 'done'
   AND EXISTS (
         SELECT 1
           FROM transcript_segments ts
          WHERE ts.episode_id = e.id
       );

-- Set completion timestamp where missing for those marked 'done'
UPDATE episodes e
   SET transcription_timestamp_completed = COALESCE(transcription_timestamp_completed, NOW())
 WHERE transcript_status = 'done';

COMMIT;
"""

if __name__ == "__main__":
    with get_db_client() as db:
        with db.conn.cursor() as cur:
            cur.execute(SQL)
        db.conn.commit()
        print("Migration complete.")