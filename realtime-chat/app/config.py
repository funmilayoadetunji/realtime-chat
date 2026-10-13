"""All tunables in one place; every value can be overridden via env var."""
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Rooms are hashed onto SHARDS streams. Same room -> same stream -> total order per room.
SHARDS = int(os.getenv("STREAM_SHARDS", "4"))
STREAM_MAXLEN = int(os.getenv("STREAM_MAXLEN", "10000"))  # approx. cap per stream

# Gateway / backpressure
CLIENT_QUEUE_SIZE = int(os.getenv("CLIENT_QUEUE_SIZE", "256"))  # per-socket outbound buffer
SEND_TIMEOUT = float(os.getenv("SEND_TIMEOUT", "5"))            # max seconds for one ws send
MAX_MSG_LEN = int(os.getenv("MAX_MSG_LEN", "2000"))
RATE_PER_SEC = float(os.getenv("RATE_PER_SEC", "5"))            # token bucket refill
RATE_BURST = int(os.getenv("RATE_BURST", "10"))

# Idempotency + replay
DEDUPE_TTL = int(os.getenv("DEDUPE_TTL", "300"))   # seconds a (user, cid) pair is remembered
REPLAY_MAX = int(os.getenv("REPLAY_MAX", "200"))   # max messages replayed after reconnect
REPLAY_SCAN = int(os.getenv("REPLAY_SCAN", "1000"))  # entries scanned per history query

# Archive worker (consumer group)
GROUP = "archivers"
DLQ_STREAM = "chat:dlq"
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
CLAIM_IDLE_MS = int(os.getenv("CLAIM_IDLE_MS", "15000"))  # reclaim msgs idle this long
DB_PATH = os.getenv("DB_PATH", "chat.db")
CHAOS_FAIL_RATE = float(os.getenv("CHAOS_FAIL_RATE", "0"))  # demo: randomly fail archiving
