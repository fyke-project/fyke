# Requirements — P1 (Free OSS Agent)

P1 is the **free, air-gapped, in-JVM agent** that ships as a Spring Boot starter. It is the wedge: every interviewee (10/10) would adopt the outbox starter even without paying for anything. See [decisions.md](decisions.md) for the "why" behind each requirement.

**Non-negotiable global rule:** the agent must be fully functional with **zero external connections** (AGENTS.md hard constraint #1). Nothing in P1 phones home.

---

## R1 — Transactional capture (same transaction)

The outbox row is written in the **same DB transaction** as the domain write. The public API:

- Annotation-driven: `@FykeEvent` on a domain method/field, **or**
- Programmatic: `Fyke.send(...)` — registers the event with the **active Spring `@Transactional` context**; the row is flushed on commit, dropped on rollback.
- If no transaction is active, the row is written in its own transaction (logged as a config smell, but never fails silently).

Delivery is **at-least-once**. The publisher dedupes on a client-supplied (or derived) `idempotency_key` via a unique constraint in `fyke_outbox`, and attaches the key to message headers for downstream consumer deduplication.

**Acceptance criteria**

- Given a transaction that commits, then the JVM is **killed before publish** → after restart, the event is delivered to the broker.
- Given two sends with the same `idempotency_key` → second send is rejected at outbox store level; broker receives the message once.
- Given a transaction that **rolls back** → no outbox row exists, nothing published.

## R2 — Efficient poller & Partitioning (the dealbreaker fix)

Naive `SELECT … FOR UPDATE` polling causes row-lock contention and I/O bloat (interviews: Markus, Priya). The poller:

- **`PgNotifyChannel`** — the Postgres zero-latency path: dedicated physical connection with `LISTEN` on the outbox channel; `NOTIFY` is a *wakeup hint only* — the poller **always re-queries** (a lost hint must never lose a message). Auto-reconnects on connection drops. Requires **Postgres ≥ 14**.
- **`TimerChannel`** — the fallback for non-Postgres JDBC DBs and idle backoff.
- **`PartitionResolver`** — SPI to assign events to partitions (`SinglePartitionResolver` for global FIFO; `BusinessKeyPartitionResolver` for per-key/tenant FIFO).
- **`PartitionLocker`** — Claims partition processing rights across multiple instances using Postgres transaction advisory locks (`pg_try_advisory_xact_lock`), with graceful in-JVM lock fallback for non-Postgres/H2.
- **`BatchClaimer`** — claims a batch via `SELECT … FOR UPDATE SKIP LOCKED` (PG), ordered by `seq`, with a **lease** (`lease_expires_at`) so crashed pollers are re-claimed.
- **Idle-safe:** near-zero CPU when there is nothing to do (NOTIFY-driven or backoff).

**Acceptance criteria**

- New event is published **≤ ~100 ms** after commit on the NOTIFY path, with a 10k-row backlog present.
- **50 concurrent committers** across partitions → no lock waits, no starvation, all batches processed in strict per-partition FIFO order.
- Idle system (no events) → near-zero CPU and minimal query volume.
- Kill the poller mid-batch (lease expires) → another poller claims the same rows; messages published.

## R3 — Binder SPI + RabbitMQ binder

The binder is an **SPI** with four operations: `publish`, `ack` (confirm/consumed), `purge`, and **DLQ-attach** (how a consumer failure reaches the DLQ). Binders consume generic `destination`, `target`, and `headers` without broker-specific coupling. **Core must never reference a concrete broker** (AGENTS.md #7).

**RabbitBinder** (first implementation):

- Publisher confirms (transactional or mandatory+confirm).
- Sends raw `payload` bytes directly to exchange (`destination`) with routing key (`target`) and `headers`.
- Exponential backoff with jitter; after `maxAttempts` → mark row **DEAD**, copy payload and metadata into `fyke_dlq`.
- `ack`/`purge` wired to the confirm/ack semantics of Rabbit.

**Acceptance criteria**

- Broker **down** at publish time → backoff, no loss; when the broker returns, the backlog drains.
- After restart → backlog drains, broker receives all confirmed messages.
- A message that always fails confirm → ends up in `fyke_dlq` with status DEAD; the poller continues with the rest of the batch.

## R4 — Consumer-side DLQ / poison capture

Fyke is not only about the *publish* side (interview: Dave). The agent captures **consumer failures**:

- Helper integration with Spring AMQP's error handling (`FykeFatalExceptionStrategy` / `MessageRecoverer`): a poison message that exhausts redeliveries lands in `fyke_dlq` with payload bytes, failure reason, attempt count, consumer name, business key, original destination, and headers.
- The consumer keeps processing **other** messages; one poison pill must not wedge the queue.
- Replayable in-JVM via `Fyke.replay(id)` back to the original destination.

**Acceptance criteria**

- A message that throws on every delivery → after N redeliveries it is in `fyke_dlq`, queryable, with reason + attempts recorded.
- While one message is poisoning, other messages on the same queue continue to process normally.
- In-JVM `Fyke.replay(id)` re-publishes the DLQ message to its original destination.

## R5 — Metadata + business-key index

Both `fyke_outbox` and `fyke_dlq` carry a standard metadata block: `partition_key`, `type`, `destination`, `target`, `business_key`, `status`, `content_type`, `created/updated/published_at`, `consumer`, `correlation_id`, `trace_id`, `size`, `payload_hash`, `headers`.

Indexes: `(business_key, status)` and `(type, status, created_at)` on both tables.

**Acceptance criteria**

- The query *"everything that happened for `order_X` — published, pending, dead"* is **one indexed query** across outbox + DLQ, and runs in ms with a 1M-row table (test includes the `EXPLAIN` check or an index-hit assertion).

## R6 — Client-side sanitization

Anything that *could* leave the JVM (telemetry, future exporters) is sanitized **in the agent, before it leaves**:

- Config-driven: field **allow/deny lists**, **hashing** of payload fields, **max preview length**.
- **Default = metadata-only**: raw payloads never appear in any emitted telemetry, ever. This is the client-side half of the SaaS/EE data-boundary decision (decisions.md D-009).

**Acceptance criteria**

- With default config, the full OTel output of a running agent contains **no payload bytes** (asserted in a test that exports to a recording OTel provider).
- With a deny-list config, the listed fields are hashed; with an allow-list, only listed fields appear.

## R7 — OpenTelemetry + graceful degradation

- Spans: `fyke.outbox.capture`, `fyke.outbox.publish`, `fyke.consume`, `fyke.dlq` — with `business_key`, `type`, `idempotency_key` as span attributes (post-sanitization).
- Metrics: **outbox backlog depth**, publish success/rate, publish latency, attempt-count histogram, **DLQ growth**.
- **Works with zero exporters** (OTel no-op pipeline) and **never blocks or fails the app's main path** — a telemetry failure is a log line, not an exception in the request path.

**Acceptance criteria**

- The demo runs with no OTel configuration at all and is unaffected.
- With an in-memory OTel exporter, the four span kinds and five metric kinds appear.

## R8 — Starter UX (zero config + Liquibase)

- `@AutoConfiguration`: a stock Boot app that adds `fyke-spring-boot-starter` + a `DataSource` (Postgres) + Spring AMQP is **fully working with zero configuration** (sensible defaults; sane outbox/DLQ table names).
- **All knobs under `fyke.*`** properties (batch size, backoff, poller channel selection, sanitization rules, etc.), with documented defaults in KDoc.
- **Liquibase** changelog (XML or YAML) **bundled inside the starter JAR** and auto-applied: creates `fyke_outbox` and `fyke_dlq` with all R5 metadata columns and indexes. No Flyway (decisions.md D-007).
- The README **quickstart** (dependency + `@Transactional` method + `@FykeEvent`) works **copy-paste**.

**Acceptance criteria**

- The quickstart is itself a test: a fixture app following the README verbatim boots and delivers an event.
- A fresh Postgres container + starter → both tables exist after startup, no manual migration step.

## R9 — Demo app (`fyke-demo`)

A standalone demo + the **CI proof**: Testcontainers **Postgres + RabbitMQ**, three scripted scenarios:

1. **Pod-death mid-transaction** — commit succeeds, JVM/publish dies → restart → delivered, no loss.
2. **Broker down** — backlog accumulates → broker recovers → drains, deduped.
3. **Poison pill** — consumer always fails → DLQ'd → **search by business key** → **`Fyke.replay(...)` in-JVM** → consumed successfully.

**Acceptance criteria**

## R10 — Retention & Housekeeping (the table-bloat fix)

High-throughput event outboxes accumulate millions of rows quickly. Without automated cleanup, Postgres tables suffer dead tuple bloat and disk exhaustion:

- Configurable retention periods: `fyke.retention.outbox-ttl` (default: 7 days) and `fyke.retention.dlq-ttl` (default: 30 days).
- Background cleanup worker: runs on a low-frequency timer (`fyke.retention.purge-interval`, default: 1 hour), deleting published outbox rows and replayed DLQ messages in bounded chunks (`batch-size`, default: 1000) to prevent lock spikes and WAL storms.

**Acceptance criteria**

- Published events older than `outbox-ttl` are purged by the cleaner.
- Unprocessed events (`NEW`, `DISPATCHING`) and unreplayed `DEAD` events are never purged.

---


## Definition of done (P1)

1. Every acceptance criterion above has a test; all green under `./gradlew build`.
2. Module layout: `fyke-core`, `fyke-binder-rabbitmq`, `fyke-spring-boot-starter`, `fyke-demo` (Gradle multi-module, Kotlin DSL).
3. KDoc on all public API; README quickstart verified by a test; `CHANGELOG.md` present.
4. Nothing in the repo phones home — no remote URLs in runtime code (AGENTS.md #1).
5. **Verify before scaffolding** (these rot — check official sources, don't trust stale snippets):
   - Latest **Spring Boot 4.x** patch (start.spring.io is the reference).
   - Boot **4 reorganized module/starter coordinates** — confirm JDBC / AMQP / OTel / Liquibase artifacts.
   - KGP (Kotlin Gradle plugin) version compatible with Boot 4.
   - Testcontainers image tags: Postgres **≥ 16**, RabbitMQ stable.
