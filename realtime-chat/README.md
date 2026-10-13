# Realtime Chat — WebSockets + Redis Streams

A horizontally scalable chat backend built around an event log. Stateless WebSocket gateways
sit in front of **Redis Streams**; a separate worker group archives messages with retries and a
dead-letter queue. Built to demonstrate concurrency control, failure handling and scaling trade-offs.

```
                    ┌────────────┐
 browser ──ws──►    │   nginx    │  (any node can serve any client)
                    └─────┬──────┘
              ┌───────────┴───────────┐
        ┌─────▼─────┐           ┌─────▼─────┐
        │ gateway 1 │           │ gateway 2 │   FastAPI + asyncio
        └──┬─────▲──┘           └──┬─────▲──┘
     XADD  │     │ XREAD     XADD  │     │ XREAD      (fan-out: every node reads every shard)
           ▼     │                 ▼     │
        ┌──────────────────────────────────┐
        │ Redis Streams: chat:stream:0..N-1│  room → crc32(room) % N
        └───────────────┬──────────────────┘
                        │ XREADGROUP (consumer group "archivers")
                  ┌─────▼─────┐   fail ×3   ┌────────────┐
                  │ worker(s) ├────────────►│ chat:dlq   │
                  └─────┬─────┘             └────────────┘
                        ▼
                     SQLite (swap for Postgres)
```

## Run it

```bash
docker compose up --build          # redis + 2 gateways + worker + nginx
open http://localhost:8080         # open two tabs; they may land on different gateways

# or locally
pip install -r requirements-dev.txt
redis-server &                     # or: docker run -p 6379:6379 redis:7
uvicorn app.main:app --port 8000 & python -m app.worker
pytest                             # unit tests use fakeredis, no server needed
CHAOS_FAIL_RATE=0.5 python -m app.worker   # watch retries and dead-lettering
```

## Protocol (JSON over WebSocket `/ws?user=<name>`)

| Client → server | Meaning |
|---|---|
| `{type:"join", room, last_id?}` | Subscribe; server replays everything after `last_id` (or the last 50) |
| `{type:"msg", room, text, cid}` | Send. `cid` is a client-generated UUID used for idempotency |
| `{type:"leave", room}` / `{type:"ping"}` | |

Server → client: `history`, `msg`, `ack {cid, id, dup}`, `error {code, retryable, cid?}`, `pong`.

## Design decisions

**1. The stream is the source of truth, not the socket.** A message is "sent" once it is appended
to a Redis Stream and acked. Delivery to sockets is a *projection* of that log. This one decision
gives ordering, replay, durability and decoupling of gateways from each other.

**2. Why Redis Streams** (vs. Pub/Sub, RabbitMQ, Kafka):

| | Pub/Sub | **Streams** | RabbitMQ | Kafka |
|---|---|---|---|---|
| Replay after reconnect | ✗ | ✓ (by ID) | ✗ | ✓ |
| Consumer groups, ack, retry | ✗ | ✓ | ✓ | ✓ |
| Ops footprint | tiny | tiny | medium | large |
| Throughput ceiling | high | high | medium | very high |

Pub/Sub loses messages whenever a node blips. Kafka is the right answer at millions of msgs/sec
but is heavy for this scale. Streams give Kafka-like semantics (offsets, groups, replay) with
one small dependency.

**3. Two read patterns on the same log.**
- *Fan-out*: each gateway runs a plain `XREAD` over all shards, because every node needs every
  message for its local sockets. No consumer group (a group would split messages between nodes).
- *Work queue*: workers use a **consumer group** so each message is archived by exactly one worker.

**4. Ordering.** Rooms hash to one of `N` streams, so each room has a total order (stream IDs are
monotonic) while different rooms parallelise. There is no global order, and none is needed.

**5. Concurrency model.** One asyncio event loop per gateway, no shared locks. Each socket has a
bounded outbound queue and its own writer task, so a slow client cannot stall the fan-out loop.
Blocking work (SQLite) goes through `asyncio.to_thread`.

## Failure handling

| Failure | Behaviour |
|---|---|
| Client drops / network flaps | Reconnect with exponential backoff + jitter, rejoin with `last_id`, server replays the gap; client de-dupes by ID |
| Ack lost, client retries | `cid` idempotency key → server returns the original ID with `dup:true`; no double post |
| Redis down during send | Gateway returns `error{retryable:true}`; client keeps the message pending and resends |
| Redis down for the fan-out loop | Backoff + jitter; the stream cursor is kept, so it resumes with no loss |
| Gateway crash | Stateless; clients reconnect to another node and replay |
| Slow consumer | Queue full → close with 1013 rather than buffer unboundedly; client reconnects and replays |
| Abusive client | Per-connection token bucket (`rate_limited`, retryable), input validation, size cap |
| Worker crash mid-message | Entry stays in the consumer group's pending list; `XAUTOCLAIM` hands it to another worker |
| Poison message | After `MAX_RETRIES` deliveries it moves to `chat:dlq` and is ACKed so it can't block the stream |
| Duplicate delivery to worker | Archive insert is `INSERT OR IGNORE` on the stream ID, so it is idempotent |

**Delivery semantics:** at-least-once end to end, made *effectively-once* by idempotent publish
(sender side) and ID-based de-duplication (receiver and archive).

## Trade-offs and known limits (deliberately explicit)

- **Redis is a single point of failure.** Production: Redis Sentinel or Cluster. Stream keys are
  independent, so sharding across a cluster works as-is. Async replication can still lose the last
  few writes on failover; if that's unacceptable, move the log to Kafka.
- **Idempotency is not perfectly atomic.** `SET NX` → `XADD` → `SET id` is three round-trips. A
  crash between `XADD` and the last `SET` can allow one duplicate after the 5s reservation expires.
  A Lua script (or `MULTI`) closes that window; I kept plain commands so the logic stays readable
  and testable with fakeredis.
- **Every gateway reads every shard.** Simple and low-latency, but fan-out read cost grows with
  nodes × total traffic. Next step: nodes subscribe only to shards that have local rooms, or route
  rooms to gateways with consistent hashing.
- **Replay filters by room within a shard** (`XRANGE` + filter, capped by `REPLAY_SCAN`). Fine for
  a few rooms per shard; at scale use one stream per large room, or read history from the DB.
- **Stream trimming** (`MAXLEN ~ 10000`) bounds memory; clients offline longer than the retained
  window miss messages, and history should then come from the archive.
- **Not included:** authentication (the `user` query param is a demo stand-in; use a JWT and
  verify it on connect), presence/typing indicators, read receipts, E2E encryption, metrics.
  Natural additions: Prometheus counters for queue depth / slow-consumer drops / DLQ size.

## Layout

```
app/main.py     gateway: validation, rate limiting, protocol handler
app/hub.py      connection registry, bounded queues, token bucket
app/broker.py   idempotent publish, replay, fan-out loop (Redis Streams)
app/worker.py   consumer-group archiver: retries, XAUTOCLAIM, DLQ
static/         minimal client with reconnect, replay and resend
tests/          fakeredis unit tests (idempotency, replay, backpressure, DLQ)
```
