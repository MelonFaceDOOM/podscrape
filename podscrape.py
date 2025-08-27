# pods_cli.py
import argparse
import os
from rss import get_unscraped_episodes, get_podnews_top_50_podcasts, update_rss_file
from scrape import download_episodes_and_save_remotely, download_episodes_and_save_locally
from db_client import get_db_client

DEFAULTS = {
    "workers_days": 7,   # fallback used by nothing here; left as example centralization
}

# ---------- simple wrappers ----------

def get_podnews_top_50(output_filepath):
    get_podnews_top_50_podcasts(output_filepath)

def scrape_episodes_from_rss_and_save_locally():
    unscraped = get_unscraped_episodes()
    print(f"scraping {len(unscraped)} podcast episodes...")
    download_episodes_and_save_locally(unscraped)

def scrape_episodes_from_rss_and_save_remotely():
    unscraped = get_unscraped_episodes()
    print(f"scraping {len(unscraped)} podcast episodes...")
    download_episodes_and_save_remotely(unscraped)

def db_ep_count():
    with get_db_client() as db:
        print(db.ep_count())

def db_recent_count():
    with get_db_client() as db:
        rows = db.recent_episode_counts()
    for day, count in rows:
        print(f"{day}: {count}")

# ---------- worker & transcription intel ----------

def db_workers():
    with get_db_client() as db:
        info = db.active_workers_info()

    print("Active workers (status='processing'):")
    if not info["workers"]:
        print("  (none)")
    else:
        for w in info["workers"]:
            print(f"  worker={w['worker_id']}  processing={w['processing']}  "
                  f"next_lease={w['next_lease_exp']}  last_lease={w['last_lease_exp']}")
    print(f"Pending episodes: {info['pending_total']}")

def _format_hms(seconds: float) -> str:
    s = int(round(seconds))
    h = s // 3600
    m = (s % 3600) // 60
    ss = s % 60
    return f"{h:02d}:{m:02d}:{ss:02d}"

def db_nth_transcription(n: int):
    from db_client import get_db_client
    with get_db_client() as db:
        rec = db.nth_most_recent_transcription(n)
        if not rec:
            print(f"No completed transcriptions found (n={n}).")
            return

        stats = db.transcript_stats(rec["id"])
        excerpt, took = db.transcript_words_excerpt(rec["id"], limit_words=1000)

        print("=== Nth Most Recent Transcription ===")
        print(f"Episode ID:   {rec['id']}")
        print(f"Audio path:   {rec['audio_path']}")
        print(f"Completed at: {rec['completed_at']}")
        print(f"Duration:     {_format_hms(stats['duration_s'])}  ({stats['duration_s']:.2f} s)")
        print(f"Word count:   {stats['word_count']:,}")
        print("")
        print("--- First 1000 words (or fewer if shorter) ---")
        print(excerpt)
        print("")
        print(f"(Printed {took} words.)")

def db_recent_transcribed(limit_days: int = 7):
    with get_db_client() as db:
        rows = db.recent_transcription_counts(limit_days)
    if not rows:
        print("No transcription completions found.")
        return
    print(f"Most recent {len(rows)} days with transcription completions:")
    for r in rows:
        print(f"  {r['day']}: {r['count']}")

# ---------- update orchestration ----------

def update_local():
    update_rss_file()
    scrape_episodes_from_rss_and_save_locally()

def update_remote():
    update_rss_file()
    scrape_episodes_from_rss_and_save_remotely()

# ---------- CLI ----------

COMMANDS = [
    {
        "name": "get_top_50",
        "help": "Write Podnews Top 50 podcasts to a file.",
        "func": lambda a: get_podnews_top_50(a.output_filepath),
        "args": [ (["output_filepath"], {"type": str}) ],
    },
    { "name": "scrape_remote", "help": "Scrape episodes from RSS and upload to remote.", "func": lambda a: scrape_remote() },
    { "name": "scrape_local",  "help": "Scrape episodes from RSS and save locally.",    "func": lambda a: scrape_local()  },
    { "name": "count",         "help": "Print total episodes in DB.",                   "func": lambda a: db_ep_count()   },
    { "name": "recent",        "help": "Episodes saved in the last week.",              "func": lambda a: db_recent_count() },
    { "name": "update_local",  "help": "Update RSS → scrape → save locally.",           "func": lambda a: update_local()  },
    { "name": "update_remote", "help": "Update RSS → scrape → save remotely.",          "func": lambda a: update_remote() },
    { "name": "workers",       "help": "Active workers and pending count.",             "func": lambda a: db_workers()    },
    {
        "name": "nth",
        "help": "Print the Nth most recent transcription (n=1 → most recent).",
        "func": lambda a: db_nth_transcription(a.n),
        "args": [ (["n"], {"type": int, "nargs": "?", "default": 1}) ],
    },
    {
        "name": "recent_transcribed",
        "help": "Counts of transcriptions for the most recent N days with completions.",
        "func": lambda a: db_recent_transcribed(a.days),
        "args": [ (["--days"], {"type": int, "default": 7}) ],
    },
]

def main():
    parser = argparse.ArgumentParser(
        description="PodScrape: RSS → episodes → storage → transcription helpers."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # build subparsers from the single COMMANDS spec
    for cmd in COMMANDS:
        p = sub.add_parser(cmd["name"], help=cmd["help"])
        for flags, kwargs in cmd.get("args", []):
            p.add_argument(*flags, **kwargs)
        p.set_defaults(_func=cmd["func"])

    args = parser.parse_args()
    args._func(args)

if __name__ == "__main__":
    main()