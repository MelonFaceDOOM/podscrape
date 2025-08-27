import os
import time
import queue
import threading
import traceback
from contextlib import ExitStack
from typing import Dict, Any, Optional

from whisper_runtime import get_word_level_model
from db_client import get_db_client
from sftp_client import get_sftp_client

# -------------------
# Config
# -------------------
DEVICE = os.getenv("ASR_DEVICE", "cuda")          # "cuda" or "cpu"
MODEL_NAME = os.getenv("ASR_MODEL", "fw_base")    # "fw_base", "fw_tiny", "oa_base"
WORKER_ID = os.getenv("HOSTNAME") or os.getenv("COMPUTERNAME") or "worker-unknown"

NUM_WORKERS = int(os.getenv("ASR_WORKERS", "1"))  # # of transcribe threads (be careful with VRAM)
PREFETCH = int(os.getenv("ASR_PREFETCH", "3"))    # max temp files queued (downloaded ahead)
CLAIM_BATCH = int(os.getenv("ASR_CLAIM_BATCH", "2"))
SLEEP_EMPTY_S = float(os.getenv("ASR_EMPTY_SLEEP", "2.0"))
LEASE_MINUTES = int(os.getenv("ASR_LEASE_MIN", "60"))  # must match your DB config

SENTINEL = object()  # queue poison pill

# -------------------
# Small helpers
# -------------------

def _fetch_episode_meta(db, episode_id: str) -> Optional[Dict[str, Any]]:
    """Return at least {'id','audio_path'} for an episode id."""
    try:
        return db.get_episode_meta(episode_id)
    except AttributeError:
        with db.conn.cursor() as cur:
            cur.execute("SELECT id, audio_path FROM episodes WHERE id = %s", (episode_id,))
            row = cur.fetchone()
        if not row:
            return None
        return {"id": row[0], "audio_path": row[1]}

def _download_to_temp(sftp, remote_path: str, dest_dir: str) -> str:
    """Download remote_path to a temp file and return local path."""
    sftp.sftp.stat(remote_path)  # raise if missing
    local_path = os.path.join(dest_dir, f"temp_{os.path.basename(remote_path)}")
    with open(local_path, "wb") as dst:
        sftp.sftp.getfo(remote_path, dst)
    return local_path

class LeaseExtender:
    """Background pinger to extend a lease while a long job runs."""
    def __init__(self, db, episode_id: str, minutes: int = LEASE_MINUTES, interval_s: int = 60):
        self.db = db
        self.episode_id = episode_id
        self.minutes = minutes
        self.interval_s = interval_s
        self._stop = threading.Event()
        self._thr = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop.wait(self.interval_s):
            try:
                self.db.extend_lease(self.episode_id, minutes=self.minutes)
            except Exception as e:
                # Non-fatal; we’ll try again on next tick
                print(f"[lease] extend failed for {self.episode_id}: {e}")

    def start(self): self._thr.start()
    def stop(self): self._stop.set()

# -------------------
# Producer (Downloader)
# -------------------

def downloader_thread(sftp, out_q: "queue.Queue", stop_event: threading.Event):
    """
    Producer: claims episodes, downloads files, enqueues {'id','path'} items.
    Honors PREFETCH strictly; claims only up to free capacity.
    """
    from db_client import get_db_client  # thread-local import

    # simple "no work" sentinel: if we see N consecutive empty polls and the queue is empty, we exit
    EMPTY_LIMIT = 3
    empty_count = 0

    try:
        with get_db_client() as db:
            while not stop_event.is_set():
                free = max(0, PREFETCH - out_q.qsize())
                if free <= 0:
                    time.sleep(0.2)
                    continue

                batch = min(CLAIM_BATCH, free)
                try:
                    ids = db.claim_episodes(WORKER_ID, batch_size=batch)
                except Exception as e:
                    print(f"[CLAIM FAIL] {e}")
                    time.sleep(1.0)
                    continue

                if not ids:
                    empty_count = empty_count + 1 if out_q.qsize() == 0 else 0
                    if empty_count >= EMPTY_LIMIT:
                        # nothing to claim AND nothing queued → producer done
                        break
                    time.sleep(SLEEP_EMPTY_S)
                    continue

                empty_count = 0

                for eid in ids:
                    if stop_event.is_set():
                        break

                    meta = _fetch_episode_meta(db, eid)
                    apath = (meta or {}).get("audio_path")
                    if not apath:
                        print(f"[META MISS] {eid}: no audio_path")
                        try:
                            db.mark_failed(eid, retry=False)
                        except Exception:
                            pass
                        continue

                    try:
                        local_path = _download_to_temp(sftp, apath, dest_dir=os.getcwd())
                    except Exception as e:
                        print(f"[DL FAIL] {eid} {apath}: {e}")
                        traceback.print_exc()
                        try:
                            db.mark_failed(eid, retry=True)
                        except Exception:
                            pass
                        continue

                    # block until space available (keeps PREFETCH bound)
                    while not stop_event.is_set():
                        try:
                            out_q.put({"id": eid, "path": local_path}, timeout=0.5)
                            break
                        except queue.Full:
                            continue

    except Exception:
        traceback.print_exc()
    # Let main thread place N sentinels for N workers once producer exits.

# -------------------
# Consumer (Transcriber)
# -------------------

def transcribe_worker(idx: int,
                      model,
                      run_fn,
                      model_lock: threading.Lock,
                      in_q: "queue.Queue",
                      stop_event: threading.Event):
    """
    Consumer: pulls items from queue, transcribes, writes to DB, cleans up.
    Uses a dedicated DB connection per worker. Serializes model use with a lock.
    """
    print(f"[worker {idx}] starting")
    from db_client import get_db_client  # thread-local import

    try:
        with get_db_client() as db:
            while not stop_event.is_set():
                item = in_q.get()  # blocking
                if item is SENTINEL:
                    # put back for other workers and exit
                    in_q.put(SENTINEL)
                    in_q.task_done()
                    print(f"[worker {idx}] stopping")
                    return

                eid = item["id"]
                local_path = item["path"]
                extender = None

                try:
                    # keep lease alive during long transcribe
                    extender = LeaseExtender(db, eid, minutes=LEASE_MINUTES, interval_s=60)
                    extender.start()

                    # serialize GPU model use (safe default)
                    with model_lock:
                        segs, words = run_fn(model, local_path)

                    # write and mark done
                    db.word_level_insert(eid, segs, words)
                    db.mark_done(eid)
                    print(f"[worker {idx}] updated: {local_path}")

                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    print(f"[worker {idx}] FAIL {eid}: {e}")
                    traceback.print_exc()
                    try:
                        # retryable; you can choose retry=False for repeated failures
                        db.mark_failed(eid, retry=True)
                    except Exception:
                        pass
                finally:
                    if extender:
                        extender.stop()
                    try:
                        if os.path.exists(local_path):
                            os.remove(local_path)
                    except Exception:
                        pass
                    in_q.task_done()
    except Exception:
        traceback.print_exc()

# -------------------
# Orchestrator
# -------------------

def transcribe_missing_episodes():
    """
    End-to-end runner:
      - opens SFTP
      - loads ASR model once (shared)
      - starts one downloader (producer) + N transcribe workers (consumers)
      - waits until producer finishes and queue drains, then sends sentinels
    """
    q = queue.Queue(maxsize=PREFETCH)
    stop_event = threading.Event()
    model_lock = threading.Lock()  # serialize model use across workers

    with ExitStack() as stack:
        db = stack.enter_context(get_db_client())          # sanity check DB only
        sftp = stack.enter_context(get_sftp_client())      # one SFTP for producer
        print(f"Transcribing with {MODEL_NAME} on {DEVICE}…")

        # Load model once and share (safe; avoids multiple VRAM loads)
        model, run_fn = get_word_level_model(MODEL_NAME, device=DEVICE)

        # Start producer
        prod = threading.Thread(target=downloader_thread, args=(sftp, q, stop_event), daemon=True)
        prod.start()

        # Start consumers
        workers = []
        for i in range(NUM_WORKERS):
            t = threading.Thread(
                target=transcribe_worker,
                args=(i+1, model, run_fn, model_lock, q, stop_event),
                daemon=True
            )
            t.start()
            workers.append(t)

        try:
            # Wait for producer to finish claiming/downloading
            prod.join()

            # Wait for queue to drain (all tasks processed)
            q.join()

        except KeyboardInterrupt:
            print("Interrupted; stopping…")
        finally:
            # Tell workers to exit
            for _ in workers:
                q.put(SENTINEL)
            stop_event.set()

            # Join workers
            for w in workers:
                w.join(timeout=5.0)

    print("All done.")

if __name__ == "__main__":
    transcribe_missing_episodes()