import asyncio

from app import config
from app.broker import STREAMS, Broker
from app.hub import Connection, Hub
from app.worker import Archive, ensure_groups, handle_entry


async def test_publish_is_idempotent(redis):
    b = Broker(redis)
    id1, dup1 = await b.publish("general", "alice", "hi", "cid-1")
    id2, dup2 = await b.publish("general", "alice", "hi", "cid-1")  # client retry
    assert (dup1, dup2) == (False, True) and id1 == id2
    assert await redis.xlen(STREAMS[0]) == 1


async def test_replay_returns_only_gap_for_that_room(redis):
    b = Broker(redis)
    first, _ = await b.publish("general", "a", "one", "1")
    await b.publish("other", "b", "noise", "2")          # same shard, different room
    await b.publish("general", "a", "two", "3")
    msgs = await b.history("general", after_id=first)
    assert [m["text"] for m in msgs] == ["two"]
    assert [m["text"] for m in await b.history("general")] == ["one", "two"]


async def test_fanout_delivers_new_messages(redis):
    b, got = Broker(redis), []
    task = asyncio.create_task(b.fanout(got.append))
    await asyncio.sleep(0.1)
    await b.publish("general", "a", "live", "1")
    for _ in range(50):
        if got: break
        await asyncio.sleep(0.05)
    task.cancel()
    assert got and got[0]["text"] == "live"


class FakeWS:
    closed = None
    async def close(self, code=1000): self.closed = code
    async def send_json(self, _): pass


async def test_slow_consumer_is_dropped_not_buffered(monkeypatch):
    monkeypatch.setattr(config, "CLIENT_QUEUE_SIZE", 2)
    ws, hub = FakeWS(), Hub()
    conn = Connection(ws, "slow")          # no writer task => never drains
    hub.join(conn, "general")
    for i in range(5):
        hub.broadcast({"room": "general", "id": f"{i}-0"})
    await asyncio.sleep(0)
    assert conn.closing and ws.closed == 1013 and conn.queue.qsize() == 2


async def test_poison_message_goes_to_dlq(redis, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "MAX_RETRIES", 0)
    await ensure_groups(redis)
    await Broker(redis).publish("general", "a", "boom", "1")
    [(stream, [(mid, f)])] = await redis.xreadgroup(config.GROUP, "c1", {STREAMS[0]: ">"}, count=1)
    await handle_entry(redis, Archive(str(tmp_path / "t.db")), stream, mid, f, reclaimed=True)
    assert await redis.xlen(config.DLQ_STREAM) == 1
    assert (await redis.xpending(stream, config.GROUP))["pending"] == 0


async def test_worker_archives_and_acks(redis, tmp_path):
    await ensure_groups(redis)
    await Broker(redis).publish("general", "a", "keep me", "1")
    arc = Archive(str(tmp_path / "t.db"))
    [(stream, [(mid, f)])] = await redis.xreadgroup(config.GROUP, "c1", {STREAMS[0]: ">"}, count=1)
    await handle_entry(redis, arc, stream, mid, f)
    await handle_entry(redis, arc, stream, mid, f)  # redelivery must be harmless
    assert arc.db.execute("SELECT count(*) FROM messages").fetchone()[0] == 1
    assert (await redis.xpending(stream, config.GROUP))["pending"] == 0
