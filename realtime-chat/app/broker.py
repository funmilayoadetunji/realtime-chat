"""Thin layer over Redis Streams: idempotent publish, replay, and cross-node fan-out."""
import asyncio
import logging
import random
import time
import zlib

from redis.asyncio import Redis
from redis.exceptions import RedisError

from . import config

log = logging.getLogger("chat.broker")

STREAMS = [f"chat:stream:{i}" for i in range(config.SHARDS)]


def stream_for(room: str) -> str:
    return STREAMS[zlib.crc32(room.encode()) % len(STREAMS)]


def to_msg(mid: str, f: dict) -> dict:
    return {"type": "msg", "id": mid, "room": f["room"], "user": f["user"],
            "text": f["text"], "ts": int(f["ts"]), "cid": f.get("cid")}


class InFlight(Exception):
    """Same (user, cid) is currently being published by another request; client should retry."""


class Broker:
    def __init__(self, redis: Redis):
        self.r = redis

    async def publish(self, room: str, user: str, text: str, cid: str) -> tuple[str, bool]:
        """Append a message to the room's stream. Returns (stream_id, was_duplicate).

        Idempotent per (user, cid): a client that never saw the ack can safely resend.
        Reserve the key first (SET NX, short TTL) -> XADD -> record the real id.
        """
        key = f"chat:dedupe:{user}:{cid}"
        if not await self.r.set(key, "", nx=True, ex=5):
            existing = await self.r.get(key)
            if existing:
                return existing, True
            raise InFlight()
        try:
            mid = await self.r.xadd(
                stream_for(room),
                {"room": room, "user": user, "text": text, "cid": cid,
                 "ts": str(int(time.time() * 1000))},
                maxlen=config.STREAM_MAXLEN, approximate=True,
            )
        except BaseException:
            try:
                await self.r.delete(key)  # release reservation so the retry can proceed
            except (RedisError, OSError):
                pass
            raise
        try:
            await self.r.set(key, mid, ex=config.DEDUPE_TTL)
        except (RedisError, OSError):
            log.warning("could not record dedupe id for %s (reservation expires in <=5s)", key)
        return mid, False

    async def history(self, room: str, after_id: str | None = None, limit: int = 50) -> list[dict]:
        """Last `limit` messages, or everything after `after_id` (reconnect replay)."""
        stream = stream_for(room)
        if after_id:
            entries = await self.r.xrange(stream, min=f"({after_id}", max="+", count=config.REPLAY_SCAN)
            msgs = [to_msg(i, f) for i, f in entries if f["room"] == room]
            return msgs[: config.REPLAY_MAX]
        entries = await self.r.xrevrange(stream, count=config.REPLAY_SCAN)
        msgs = [to_msg(i, f) for i, f in entries if f["room"] == room][:limit]
        msgs.reverse()
        return msgs

    async def fanout(self, on_message) -> None:
        """Tail every shard and hand each entry to `on_message` (sync callable).

        Plain XREAD (not a consumer group) because *every* gateway node needs *every*
        message to deliver to its own sockets. The cursor survives Redis outages, so a
        reconnect resumes exactly where we stopped.
        """
        cursors = {s: "$" for s in STREAMS}
        backoff = 0.5
        while True:
            try:
                resp = await self.r.xread(cursors, count=200, block=5000)
                backoff = 0.5
                for stream, entries in resp or []:
                    for mid, f in entries:
                        cursors[stream] = mid
                        on_message(to_msg(mid, f))
            except asyncio.CancelledError:
                raise
            except (RedisError, OSError) as e:
                log.warning("fanout: redis error (%s); retrying in ~%.1fs", e, backoff)
                await asyncio.sleep(backoff + random.random() * backoff)  # jittered backoff
                backoff = min(backoff * 2, 10)
