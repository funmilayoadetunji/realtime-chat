import os
os.environ["STREAM_SHARDS"] = "1"  # one shard so tests can exercise per-room filtering
import fakeredis.aioredis
import pytest_asyncio


@pytest_asyncio.fixture
async def redis():
    r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield r
    await r.aclose()
