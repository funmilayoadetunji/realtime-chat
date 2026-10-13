"""In-process registry of WebSocket connections with per-connection backpressure."""
import asyncio
import logging
import time

from . import config

log = logging.getLogger("chat.hub")


class TokenBucket:
    def __init__(self, rate: float, burst: int):
        self.rate, self.burst = rate, burst
        self.tokens, self.t = float(burst), time.monotonic()

    def allow(self) -> bool:
        now = time.monotonic()
        self.tokens = min(self.burst, self.tokens + (now - self.t) * self.rate)
        self.t = now
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False


class Connection:
    """One socket + a bounded outbound queue. A dedicated writer task drains the queue,
    so one slow client can never block the fan-out loop or other clients."""

    def __init__(self, ws, user: str):
        self.ws, self.user = ws, user
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=config.CLIENT_QUEUE_SIZE)
        self.rooms: set[str] = set()
        self.closing = False

    def send(self, payload: dict) -> bool:
        try:
            self.queue.put_nowait(payload)
            return True
        except asyncio.QueueFull:
            # Slow consumer: drop the connection rather than buffer unboundedly.
            # The client reconnects and replays from its last seen id, so nothing is lost.
            if not self.closing:
                self.closing = True
                log.warning("slow consumer %s: closing (1013)", self.user)
                asyncio.create_task(self.ws.close(code=1013))
            return False

    async def writer(self) -> None:
        while True:
            payload = await self.queue.get()
            try:
                await asyncio.wait_for(self.ws.send_json(payload), config.SEND_TIMEOUT)
            except Exception:
                self.closing = True
                try:
                    await self.ws.close(code=1011)
                except Exception:
                    pass
                return


class Hub:
    def __init__(self):
        self.rooms: dict[str, set[Connection]] = {}

    def join(self, conn: Connection, room: str) -> None:
        self.rooms.setdefault(room, set()).add(conn)
        conn.rooms.add(room)

    def leave(self, conn: Connection, room: str) -> None:
        conn.rooms.discard(room)
        members = self.rooms.get(room)
        if members:
            members.discard(conn)
            if not members:
                del self.rooms[room]

    def remove(self, conn: Connection) -> None:
        for room in list(conn.rooms):
            self.leave(conn, room)

    def broadcast(self, msg: dict) -> None:
        for conn in list(self.rooms.get(msg["room"], ())):
            conn.send(msg)
