# Architecture — Fyke Agent (P1)

All components live **in the app's JVM** (AGENTS.md #1 air-gap-first, #2 actions in-JVM). There is no external system in P1.

## Components

```
┌────────────────────────────────────── app JVM ──────────────────────────────────────┐
│                                                                                      │
│  Domain code                                                                        │
│  (your @Transactional service)                                                       │
│       │  ① same DB transaction                                                      │
│       ▼                                                                             │
│  ┌─────────────┐        ┌──────────────────────────────┐                             │
│  │ OutboxWriter │───────▶│ fyke_outbox (Postgres)       │                             │
│  │ (capture)    │  insert│  + fyke_dlq                  │                             │
│  └─────────────┘        └──────────────┬───────────────┘                             │
│        ▲                               │ ②                                          │
│        │ transaction-local             │       ┌──────────────────────────┐          │
│        └──────────── OutboxStore ──────│──────▶│ Poller                    │          │
│                     (claim/query)       │       │  NotificationSource:      │          │
│                                         │       │   PgNotifyChannel (LISTEN/│          │
│                                         │       │   NOTIFY = hint only)     │          │
│                                         │       │   TimerChannel (fallback, │          │
│                                         │       │   interval + backoff)     │          │
│                                         │       │  BatchClaimer:            │          │
│                                         │       │   SELECT…FOR UPDATE        │          │
│                                         │       │   SKIP LOCKED, lease      │          │
│                                         │       └────────────┬─────────────┘          │
│                                         │                    │ ③ claimed batch        │
│                                         │                    ▼                        │
│                                         │       ┌──────────────────────────┐          │
│                                         │       │ Binder SPI                │          │
│                                         │       │  publish/ack/purge/       │          │
│                                         │       │  DLQ-attach               │          │
│                                         │       │   └─ RabbitBinder        │          │
│                                         │       │      (confirms, backoff, │          │
│                                         │       │       DEAD→fyke_dlq)     │          │
│                                         │       └────────────┬─────────────┘          │
│                                         │                    │ ④                        │
│                                         ▼                    ▼                        │
│                                   (status updates:     ┌────────────┐                 │
│                                    PUBLISHED/          │  Broker     │                 │
│                                    CONSUMED/DEAD)      │ RabbitMQ   │                 │
│                                                        └─────┬──────┘                 │
│                                                              │ ⑤ delivery              │
│                                                        ┌─────▼──────┐                  │
│                                                        │ Consumers   │                 │
│                                                        │  │          │                  │
│                                                        │  ▼ failures │                  │
│                                                        │ DLX/DLQ ────┼──▶ fyke_dlq     │
│                                                        └──────────────┘   (R4 capture)  │
│                                                                                        │
│  Fyke.replay(...)  ◀── in-JVM API: re-publish a stored row via the binder               │
│  OTel (spans + metrics, sanitized per R6, no-op-safe)                                  │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

The `OutboxWriter` interface is the **reactive seam** (decisions.md D-005): P1 ships the JDBC implementation (synchronous, transaction-local); an R2DBC implementation slots in later without touching anything downstream of `OutboxStore`.

## Key flows

**Happy path.** `@Transactional` service method → `@FykeEvent` registers on the tx → on commit, the outbox row (status `NEW`) is already in the DB → `NOTIFY` fires in the same tx → poller wakes, claims the batch (`SKIP LOCKED`, `seq` order) → binder publishes → publisher confirm → row → `PUBLISHED` → consumer ack → `CONSUMED`.

**Publish failure.** Confirm fails → backoff with jitter → attempts < max: retry; attempts ≥ max: row → `DEAD`, payload copied to `fyke_dlq`. The batch moves on — one failure never wedges the poller.

**Pod death (crash mid-flight).** A claimed row holds a lease (`lease_expires_at`). Crash → lease expires → the same or another poller re-claims it. At-least-once holds; broker-side dedup keeps consumers sane.

**Poison pill (consumer side).** Consumer throws on redelivery N times → Spring AMQP error handler routes to DLX → Fyke's DLQ-capture writes `fyke_dlq` (payload, reason, attempts, consumer, business key) → queue keeps draining other messages.

**Replay (in-JVM).** `Fyke.replay(key or id)` — loads the row (outbox or DLQ) and re-publishes via the binder. No DB write access granted to callers beyond what the agent mediates; this is the P1-local form of what the P2 portal will relay remotely.

## P2 preview (not built in P1)

One **agent-initiated gRPC bidirectional stream**, three flows:

1. **up** — OTLP telemetry (standard OTel/gRPC; reuses R6 sanitization).
2. **down** — signed commands (replay/ack/purge; P1's `Fyke.replay` becomes the in-JVM execution sink for relayed commands).
3. **up, on-demand** — content fetch, **self-hosted mode only** (SaaS mode: agent is configured to never send payloads).

**P1 only produces the data these flows consume** (the R5 metadata block, OTel spans, local DLQ state) and exposes the **action sinks** (`replay`/`ack`/`purge`) as a clean internal API — but P1 contains **no gRPC channel, no client, no endpoint**. This keeps P1 air-gapped while making P2 a bolt-on instead of a rewrite.

## Data model (Liquibase, decisions.md D-007)

```
fyke_outbox (
  id              UUID PK,
  seq             BIGINT,            -- append order, claim ordering
  type            TEXT,              -- event type / routing key
  business_key    TEXT,              -- user-facing key (order_X, device_id…)
  idempotency_key TEXT,              -- dedup
  status          TEXT,              -- NEW | DISPATCHING | PUBLISHED | CONSUMED | DEAD
  payload         JSONB,             -- raw event
  payload_hash    TEXT,              -- integrity + dedup proof
  consumer        TEXT NULL,
  correlation_id  TEXT NULL,
  trace_id        TEXT NULL,
  size            INT,
  created_at      TIMESTAMPTZ,
  updated_at      TIMESTAMPTZ,
  published_at    TIMESTAMPTZ NULL,
  consumed_at     TIMESTAMPTZ NULL,
  attempts        INT DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NULL,
  lease_expires_at TIMESTAMPTZ NULL
)
-- indexes: (business_key, status), (type, status, created_at),
--          (status, next_attempt_at) for the claim query, (lease_expires_at) for re-claim

fyke_dlq (
  id            UUID PK,
  source        TEXT,                -- OUTBOX | CONSUMER
  outbox_id     UUID NULL,           -- link back if publish-side
  type          TEXT,
  business_key  TEXT,
  payload       JSONB,
  reason        TEXT,                -- failure / last error
  consumer      TEXT NULL,
  received_at   TIMESTAMPTZ,
  replayed_at   TIMESTAMPTZ NULL
)
-- indexes: (business_key, source, received_at), (type, received_at)
```

Note the `fyke_dlq` table does double duty: publish-side DEAD rows (R3) and consumer-side poison messages (R4) land in the same table, distinguished by `source` — that's what makes "everything that happened for `order_X`" a single R5 query.
