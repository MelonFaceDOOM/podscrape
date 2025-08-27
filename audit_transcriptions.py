# audit_transcriptions.py
"""
Quick sanity checks for transcription recency + data health.

What it checks:
1) Counts: total episodes, done-by-timestamp, done-by-status, done-with-null-timestamp.
2) Nth-most-recent sanity: fetches nth rows directly from DB; verifies uniqueness across a range.
3) Transcript lengths: summarizes per-episode transcript duration (sum(end_s - start_s))
   and flags suspicious masses of zero/identical durations.
4) Cross-check: episodes marked done but missing segments.

Usage:
  python audit_transcriptions.py [--max-n 300]
"""

import argparse
from collections import Counter, defaultdict
from statistics import median
from datetime import datetime, timezone

from db_client import get_db_client


def fetch_one(cur, sql, params=None):
    cur.execute(sql, params or ())
    r = cur.fetchone()
    return r[0] if r else None


def audit_counts(conn):
    with conn.cursor() as cur:
        total_eps = fetch_one(cur, "SELECT COUNT(*) FROM episodes")
        done_ts = fetch_one(cur, "SELECT COUNT(*) FROM episodes WHERE transcription_timestamp_completed IS NOT NULL")
        done_status = fetch_one(cur, "SELECT COUNT(*) FROM episodes WHERE transcript_status = 'done'")
        done_null_ts = fetch_one(cur, """
            SELECT COUNT(*)
              FROM episodes
             WHERE transcript_status = 'done'
               AND transcription_timestamp_completed IS NULL
        """)
        pending = fetch_one(cur, "SELECT COUNT(*) FROM episodes WHERE transcript_status = 'pending'")
        processing = fetch_one(cur, "SELECT COUNT(*) FROM episodes WHERE transcript_status = 'processing'")
        failed = fetch_one(cur, "SELECT COUNT(*) FROM episodes WHERE transcript_status = 'failed'")

        # Optional: earliest & latest completion timestamps
        min_ts = fetch_one(cur, "SELECT MIN(transcription_timestamp_completed) FROM episodes WHERE transcription_timestamp_completed IS NOT NULL")
        max_ts = fetch_one(cur, "SELECT MAX(transcription_timestamp_completed) FROM episodes WHERE transcription_timestamp_completed IS NOT NULL")

    print("\n=== COUNTS ===")
    print(f"episodes.total                              : {total_eps:,}")
    print(f"episodes.done (status='done')               : {done_status:,}")
    print(f"episodes.done (timestamp NOT NULL)          : {done_ts:,}")
    print(f"episodes.done BUT timestamp IS NULL         : {done_null_ts:,}")
    print(f"episodes.pending                            : {pending:,}")
    print(f"episodes.processing                         : {processing:,}")
    print(f"episodes.failed                             : {failed:,}")
    if max_ts:
        print(f"completion range                            : {min_ts} → {max_ts}")
    return {"total": total_eps, "done_ts": done_ts, "done_status": done_status}


def nth_most_recent_ids(conn, upto_n=200):
    """
    Return [ (n, episode_id) ... ] for n=1..upto_n.
    Uses completed timestamp; returns None for n > available rows.
    """
    out = []
    with conn.cursor() as cur:
        for n in range(1, upto_n + 1):
            off = n - 1
            cur.execute(
                """
                SELECT id
                  FROM episodes
                 WHERE transcription_timestamp_completed IS NOT NULL
                 ORDER BY transcription_timestamp_completed DESC, id DESC
                 OFFSET %s
                 LIMIT 1
                """,
                (off,),
            )
            row = cur.fetchone()
            out.append((n, row[0] if row else None))
    return out


def audit_nth(conn, max_n=300):
    print("\n=== Nth-most-recent transcription sanity ===")
    rows = nth_most_recent_ids(conn, upto_n=max_n)
    ids = [eid for (_, eid) in rows if eid is not None]
    first_none_at = next((n for (n, eid) in rows if eid is None), None)

    unique_ids = set(ids)
    dupes = [eid for (eid, c) in Counter(ids).items() if c > 1]

    print(f"requested N up to                          : {max_n}")
    print(f"returned (non-null)                        : {len(ids)}")
    if first_none_at:
        print(f"first N with NO ROW                        : {first_none_at} (expected if N > completed rows)")
    else:
        print("no gaps (every N returned a row)")

    print(f"unique IDs among returned                  : {len(unique_ids)}")
    if dupes:
        print(f"⚠ duplicates found among the returned N   : {len(dupes)} (this indicates query or ordering issues)")
        # show a couple examples
        show = dupes[:5]
        print(f"  sample dup IDs: {show}")
    else:
        print("no duplicates among returned N (good)")

    # Show the first 5 and last 5 IDs we got, for eyeballing
    head = rows[:5]
    tail = rows[-5:]
    print("head (N,id):", head)
    print("tail (N,id):", tail)


def audit_transcript_lengths(conn):
    """
    Summarize per-episode duration = sum(end_s - start_s) from transcript_segments.
    Flags episodes with no segments and large groups of identical durations.
    """
    print("\n=== Transcript length summary (seconds) ===")
    with conn.cursor() as cur:
        # lengths for completed episodes only
        cur.execute(
            """
            SELECT e.id, COALESCE(SUM(GREATEST(0, s.end_s - s.start_s)), 0) AS duration_s,
                   COUNT(s.*) AS seg_count
              FROM episodes e
         LEFT JOIN transcript_segments s ON s.episode_id = e.id
             WHERE e.transcription_timestamp_completed IS NOT NULL
          GROUP BY e.id
            """
        )
        rows = cur.fetchall()

    if not rows:
        print("No completed episodes to analyze.")
        return

    durations = [float(r[1]) for r in rows]
    seg_counts = [int(r[2]) for r in rows]

    total = len(rows)
    zero_len = sum(1 for d in durations if d <= 0.01)
    no_segments = sum(1 for c in seg_counts if c == 0)

    mins = min(durations)
    med = median(durations)
    maxs = max(durations)

    print(f"episodes analyzed                          : {total:,}")
    print(f"duration sec (min/median/max)              : {mins:.1f} / {med:.1f} / {maxs:.1f}")
    print(f"zero/near-zero duration episodes           : {zero_len:,}")
    print(f"episodes with 0 segments                   : {no_segments:,}")

    # Large identical-duration clusters may indicate a bug (e.g., all ~123.456)
    buckets = Counter(round(d, 2) for d in durations)
    heavy = [(k, c) for (k, c) in buckets.items() if c >= 10]  # adjust threshold
    heavy.sort(key=lambda x: -x[1])
    if heavy:
        print("⚠ Frequent identical durations (>=10 eps share same length):")
        for k, c in heavy[:10]:
            print(f"  duration={k}s  count={c}")
    else:
        print("No suspicious clusters of identical durations.")

    # Show a few shortest / longest for inspection
    rows_sorted = sorted(rows, key=lambda r: float(r[1]))
    print("shortest 5 (id, sec, segs):", [(r[0], round(float(r[1]), 1), int(r[2])) for r in rows_sorted[:5]])
    print("longest 5  (id, sec, segs):", [(r[0], round(float(r[1]), 1), int(r[2])) for r in rows_sorted[-5:]])


def audit_done_without_segments(conn):
    print("\n=== Done episodes without segments ===")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT e.id
              FROM episodes e
         LEFT JOIN transcript_segments s ON s.episode_id = e.id
             WHERE e.transcription_timestamp_completed IS NOT NULL
          GROUP BY e.id
            HAVING COUNT(s.*) = 0
            """
        )
        rows = cur.fetchall()
    if rows:
        print(f"⚠ {len(rows)} completed episodes have ZERO segments.")
        print("  sample:", [r[0] for r in rows[:10]])
    else:
        print("All completed episodes have at least one segment. ✅")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-n", type=int, default=300, help="How deep to probe nth-most-recent list.")
    args = ap.parse_args()

    with get_db_client() as db:
        counts = audit_counts(db.conn)
        audit_nth(db.conn, max_n=args.max_n)
        audit_transcript_lengths(db.conn)
        audit_done_without_segments(db.conn)


if __name__ == "__main__":
    main()
