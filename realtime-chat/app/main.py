"""WebSocket gateway. Stateless: any node can serve any client."""
import asyncio
import json
import logging
import re
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import RedisError

from . import config
from .broker import Broker, InFlight
from .hub import Connection, Hub, TokenBucket

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("chat.gateway")

ROOM_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
ID_RE = re.compile(r"^\d+-\d+$")
hub = Hub()


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = Redis.from_url(
        config.REDIS_URL, decode_responses=True, health_check_interval=15,
        socket_connect_timeout=3, retry=Retry(ExponentialBackoff(), 3), retry_on_timeout=True,
    )
    app.state.redis = redis
    app.state.broker = Broker(redis)
    fan = asyncio.create_task(app.state.broker.fanout(hub.broadcast))
    yield
    fan.cancel()
    await redis.aclose()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    try:
        await app.state.redis.ping()
        return {"status": "ok"}
    except (RedisError, OSError):
        return JSONResponse({"status": "redis_unavailable"}, status_code=503)


def err(conn: Connection, code: str, detail: str, cid: str | None = None, retryable: bool = False):
    conn.send({"type": "error", "code": code, "detail": detail, "cid": cid, "retryable": retryable})


async def handle(conn: Connection, data: dict, limiter: TokenBucket, broker: Broker) -> None:
    kind = data.get("type")
    room = data.get("room")

    if kind == "ping":
        conn.send({"type": "pong"})

    elif kind == "join":
        if not isinstance(room, str) or not ROOM_RE.match(room):
            return err(conn, "bad_room", "room must match [A-Za-z0-9_-]{1,32}")
        last_id = data.get("last_id")
        last_id = last_id if isinstance(last_id, str) and ID_RE.match(last_id) else None
        # Register for live traffic *before* loading history so nothing falls in the gap.
        # The client de-dupes by id and sorts, so overlap is harmless.
        hub.join(conn, room)
        try:
            msgs = await broker.history(room, last_id)
        except (RedisError, OSError):
            msgs = []
            err(conn, "unavailable", "history temporarily unavailable", retryable=True)
        conn.send({"type": "history", "room": room, "messages": msgs})

    elif kind == "leave":
        if isinstance(room, str):
            hub.leave(conn, room)

    elif kind == "msg":
        cid, text = data.get("cid"), data.get("text")
        if not isinstance(cid, str) or not (1 <= len(cid) <= 64):
            return err(conn, "bad_request", "cid required (1-64 chars)")
        if not isinstance(room, str) or room not in conn.rooms:
            return err(conn, "not_joined", "join the room first", cid)
        if not isinstance(text, str) or not text.strip() or len(text) > config.MAX_MSG_LEN:
            return err(conn, "bad_text", f"text must be 1-{config.MAX_MSG_LEN} chars", cid)
        if not limiter.allow():
            return err(conn, "rate_limited", "slow down", cid, retryable=True)
        try:
            mid, dup = await broker.publish(room, conn.user, text.strip(), cid)
        except InFlight:
            return err(conn, "in_flight", "duplicate in progress", cid, retryable=True)
        except (RedisError, OSError):
            return err(conn, "unavailable", "message broker unavailable", cid, retryable=True)
        conn.send({"type": "ack", "cid": cid, "id": mid, "dup": dup})
    else:
        err(conn, "bad_request", f"unknown type {kind!r}")


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket, user: str = Query(..., min_length=1, max_length=32)):
    await ws.accept()
    conn = Connection(ws, user)
    writer = asyncio.create_task(conn.writer())
    limiter = TokenBucket(config.RATE_PER_SEC, config.RATE_BURST)
    try:
        while True:
            raw = await ws.receive_text()
            try:
                data = json.loads(raw)
                if not isinstance(data, dict):
                    raise ValueError
            except ValueError:
                err(conn, "bad_json", "expected a JSON object")
                continue
            await handle(conn, data, limiter, ws.app.state.broker)
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        hub.remove(conn)
        writer.cancel()


app.mount("/", StaticFiles(directory="static", html=True), name="static")
