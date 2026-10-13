"""Archive worker: durable consumer-group processing with retries and a dead-letter stream.

Run N copies for throughput; Redis load-balances entries across consumers in the group.
Delivery is at-least-once; the INSERT is idempotent (primary key = stream id).
"""
import asyncio
import logging
import os
import random
import sqlite3
import threading
import time
import uuid

from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError

from . import config
from .broker import STREAMS

log = logging.getLogger("chat.worker")


class Archive:
    def __init__(self, path: str):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        self.db.execute("CREATE TABLE IF NOT EXISTS messages("
                        "id TEXT PRIMARY KEY, room TEXT, user TEXT, text TEXT, ts INTEGER)")
        self.db.commit()

    def insert(self, mid: str, f: dict) -> None:
        if random.random() < config.CHAOS_FAIL_RATE:
            raise RuntimeError("chaos: simulated archive failure")
        with self.lock:
            self.db.execute("INSERT OR IGNORE INTO messages VALUES (?,?,?,?,?)",
                            (mid, f["room"], f["user"], f["text"], int(f["ts"])))
            self.db.commit()


async def ensure_groups(r: Redis) -> None:
    for s in STREAMS:
        try:
            await r.xgroup_create(s, config.GROUP, id="0", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise


async def handle_entry(r: Redis, archive: Archive, stream: str, mid: str, f: dict,
                       reclaimed: bool = False) -> None:
    if reclaimed:
        info = await r.xpending_range(stream, config.GROUP, min=mid, max=mid, count=1)
        deliveries = info[0]["times_delivered"] if info else 1
        if deliveries > config.MAX_RETRIES:
            await r.xadd(config.DLQ_STREAM, {**f, "src_stream": stream, "src_id": mid,
                                             "reason": f"failed {deliveries - 1} times"})
            await r.xack(stream, config.GROUP, mid)
            log.error("dead-lettered %s after %d deliveries", mid, deliveries - 1)
            return
    try:
        await asyncio.to_thread(archive.insert, mid, f)
    except Exception as e:
        # Leave it un-ACKed: it stays in the PEL and is reclaimed after CLAIM_IDLE_MS,
        # which doubles as a natural retry delay.
        log.warning("archive failed for %s (%s); will retry", mid, e)
        return
    await r.xack(stream, config.GROUP, mid)


async def run() -> None:
    r = Redis.from_url(config.REDIS_URL, decode_responses=True)
    archive = Archive(config.DB_PATH)
    consumer = f"worker-{os.getpid()}-{uuid.uuid4().hex[:6]}"
    last_reclaim, backoff = 0.0, 0.5
    await ensure_groups(r)
    log.info("worker %s started", consumer)
    while True:
        try:
            if time.monotonic() - last_reclaim > 5:  # recover entries from crashed/failed consumers
                last_reclaim = time.monotonic()
                for s in STREAMS:
                    res = await r.xautoclaim(s, config.GROUP, consumer,
                                             min_idle_time=config.CLAIM_IDLE_MS, start_id="0-0", count=50)
                    for mid, f in res[1]:
                        if f:  # None if the entry was trimmed away
                            await handle_entry(r, archive, s, mid, f, reclaimed=True)
            resp = await r.xreadgroup(config.GROUP, consumer, {s: ">" for s in STREAMS},
                                      count=50, block=2000)
            for s, entries in resp or []:
                for mid, f in entries:
                    await handle_entry(r, archive, s, mid, f)
            backoff = 0.5
        except asyncio.CancelledError:
            raise
        except (RedisError, OSError) as e:
            log.warning("redis error (%s); retrying", e)
            await asyncio.sleep(backoff + random.random() * backoff)
            backoff = min(backoff * 2, 10)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
