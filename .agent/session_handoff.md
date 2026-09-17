# Session Handoff: Fyke (P1)

## 1. Project Overview & Context

**Fyke** is a transactional outbox and reliability agent for Spring Boot 4.x (built with Kotlin on JVM 21). Its primary guarantee is that domain events are never lost: they are persisted to PostgreSQL in the exact same SQL transaction as domain data, dispatched with sub-second latency to a message broker, and can be searched and replayed in-JVM if failures occur.

### Non-Negotiable Constraints (from `AGENTS.md`)
- **Air-gap first**: 100% functional with zero external network connections or phone-home telemetry.
- **In-JVM actions**: All publishing, replay, ack, and purging operations execute directly within the application's JVM.
- **Spring Boot 4.x only**: Targeted Spring Boot 4.1.1 (no Spring Boot 3.x backward-compatibility compromises).
- **Postgres-first**: Leverages PostgreSQL transactional `NOTIFY` and `pg_try_advisory_xact_lock` with `SKIP LOCKED`. Fallback periodic timer provided for generic JDBC databases.
- **Liquibase**: Schema migrations bundled inside the starter JAR (`db/changelog/db.changelog-master.xml`).
- **Broker Binder SPI**: Core engine never references a concrete broker. RabbitMQ is the first implemented binder; Kafka is planned next.
- **Real Integration Tests**: Verified using Testcontainers (`postgres:16-alpine` and `rabbitmq:3.13-management-alpine`). Never mock the broker for reliability tests.

---

## 2. What We Have Done So Far

### A. Architectural Decisions & Requirements Refinements
Updated `docs/requirements-p1.md`, `docs/architecture.md`, and `docs/decisions.md`:
- **ADR D-012 (Logical Partitioning)**: Introduced `partition_key` and `PartitionResolver` to allow ordering per entity/aggregate/tenant rather than locking the entire outbox table.
- **ADR D-013 (Generic Broker Destination)**: Generalized fields to `destination`, `target`, and `headers` to keep core broker-agnostic across RabbitMQ, Kafka, and future transports.
- **ADR D-014 (Binary Payload Storage)**: Switched payload storage to `BYTEA` (`byte[]`) with `content_type` to allow JSON, Protobuf, Avro, and preserve byte-exact integrity for replaying.
- **ADR D-015 (Retention & Housekeeping)**: Added automated chunked cleanup (`RetentionCleaner`) for expired published and replayed records to prevent table bloat.
- **ADR D-016 (Dedicated Physical Connection for PG NOTIFY)**: Non-pooled connection dedicated to `LISTEN fyke_outbox` with exponential reconnect backoff, and graceful fallback to interval polling.

### B. Module Implementation

1. **`fyke-core`**:
   - **Liquibase Changelogs**: Created `fyke_outbox` and `fyke_dlq` tables with composite indexes on `(partition_key, status, seq)`, `(business_key, status)`, and `(type, status, created_at)`.
   - **Models**: `OutboxRecord`, `OutboxEvent`, `OutboxStatus`, `DlqRecord`, `DlqSource`, and `FykeRecordSummary`.
   - **Broker SPI**: `BrokerBinder` interface and `PublishResult` (Success, TransientFailure, DeadLetter).
   - **Storage & Atomicity**: `JdbcOutboxWriter` writes to `fyke_outbox` in the current transaction and invokes `pg_notify` upon commit. `JdbcOutboxStore` manages batch claiming with `FOR UPDATE SKIP LOCKED`, status transitions, DLQ insertions, and business key searches.
   - **Partitioning**: `PartitionResolver` (SinglePartition and BusinessKeyPartition strategies) and `PartitionLocker` (`PostgresAdvisoryPartitionLocker` using `pg_try_advisory_xact_lock(hashtext(?))` and `SingleWorkerPartitionLocker` fallback).
   - **Wakeup & Polling Engine**: `NotificationSource` with `PgNotifyChannel` (LISTEN/NOTIFY) and `TimerChannel` (periodic fallback). `PollerEngine` orchestrates partition claiming, dispatching via binder, exponential retry backoff, and in-JVM replay.
   - **Retention**: `RetentionCleaner` periodically purges old published outbox and replayed DLQ rows in bounded batches.
   - **Telemetry**: `FykeTelemetry` (OpenTelemetry metrics/spans) with `ClientSideSanitizer` (allow/deny lists, payload hashing).
   - **Unit Tests**: Full test coverage for partitioning, locks, serialization, and sanitization.

2. **`fyke-binder-rabbitmq`**:
   - `RabbitBinder`: Implements `BrokerBinder` using Spring AMQP `RabbitTemplate` and mandatory publisher confirms (`CorrelationData.future.get(confirmTimeoutMs)`).
   - `FykeRabbitDlqRecoverer`: Spring AMQP `MessageRecoverer` that captures poison-pill consumer exceptions directly into `fyke_dlq` with payload, error message, stack trace, and routing metadata.
   - **Unit Tests**: Full test coverage for ACK/NACK handling and DLQ recovery.

3. **`fyke-spring-boot-starter`**:
   - `FykeAutoConfiguration`: Auto-configuration wired after Spring Boot 4's `DataSourceAutoConfiguration` and `RabbitAutoConfiguration`.
   - `FykeProperties`: Configuration properties under `fyke.*` for poller, retention, rabbitmq, and sanitization.
   - Annotation Capture: `@FykeEvent` annotation and `FykeEventDispatcher` bean listening for Spring application events.
   - Static Facade: `Fyke.send(...)`, `Fyke.replay(id)`, and `Fyke.searchByBusinessKey(businessKey)`.
   - **Unit Tests**: Facade initialization and delegation tests.

4. **`fyke-demo` & Verification**:
   - Spring Boot demonstration application with `OrderService`, `OrderConsumer`, RabbitMQ exchange/queue bindings, and a REST `DemoController` (`POST /orders?poison=true|false`).
5. **Transactional Inbox & Consumer-Side Ordering**:
   - **Liquibase**: `02-create-inbox-table.sql` creating `fyke_inbox` table with indexes for batch claiming, business key query, retention, and `inbox_id` in `fyke_dlq`.
   - **Models**: `InboxRecord`, `InboxStatus`, `OrderingMode` (`STRICT_FIFO`, `LEAPFROG`).
   - **3-Tiered Partition Resolution**: `ConsumerPartitionResolver` (property extraction, header fallback, custom bean SPI).
   - **Annotation**: `@FykeListener(destination, consumerGroup, ordering, partitionKeyProperty, partitionResolverBean, concurrency)`.
   - **Storage & Outbox Tightening**: `JdbcInboxStore` with dynamic `claimBatch` supporting `STRICT_FIFO` via `NOT EXISTS` and `LEAPFROG`. Tightened `JdbcOutboxStore.claimBatch` with `NOT EXISTS` to prevent outbox leapfrogging during retry backoff.
   - **In-JVM Poller Engine**: `InboxPollerEngine` with partition locking, deserialization via `FykePayloadSerializer`, instant fatal poison-pill fast-path to DLQ, and exponential backoff.
   - **RabbitMQ Integration**: `RabbitConsumerRegistrar` auto-wires containers, saves raw message to `fyke_inbox`, ACKs RabbitMQ immediately, and wakes up the inbox poller.
   - **Facade**: `Fyke.retryInbox(id)` and extended `Fyke.searchByBusinessKey`.
   - `FykeScenariosTest`: Testcontainers suite running real PostgreSQL 16 and RabbitMQ 3.13 containers verifying 9 scenarios:
     1. Pod death mid-transaction recovery (outbox orphaned row claimed and published).
     2. End-to-end transactional publish and consumer receipt.
     3. Consumer poison-pill DLQ capture and in-JVM replay.
     4. Idempotency key deduplication.
     5. Transaction rollback leaves no outbox records.
     6. End-to-end Transactional Inbox receipt via `@FykeListener`.
     7. Transactional Inbox strict per-partition FIFO with retry backoff and instant unblock.
     8. Transactional Inbox fatal poison pill fast-paths directly to DLQ without retries.
     9. Producer outbox prevents leapfrogging when earlier partition record is in retry backoff.

5. **Tooling, Code Quality & Documentation**:
   - Configured Spotless plugin (`com.diffplug.spotless:8.10.2`) for Kotlin (tabs), YAML, SQL, XML, and Markdown.
   - Updated `.editorconfig` and updated `.gitignore` to exclude local scratch/container DB volumes (`.dev/`).
   - Comprehensive KDoc added to all public APIs.
   - `README.md` rewritten with copy-paste quickstart, architecture diagram, and usage examples.
   - `CHANGELOG.md` created for version `0.1.0-SNAPSHOT`.

---

6. **LISTEN/NOTIFY for Inbox & Unified Poller Pipeline**:
   - **`AbstractPollerEngine<T>`**: Unified base class managing lifecycle, `NotificationSource` attachments, non-blocking drain loops, worker pool concurrency (`concurrency > 1`), and per-partition mutual exclusion via `PartitionLocker`.
   - **`BackoffPolicy`**: Encapsulates exponential backoff calculation with multiplier and jitter.
   - **Zero-Latency Inbox Notification**: `JdbcInboxStore` issues `SELECT pg_notify('fyke_inbox_events', '1')` on PostgreSQL. `InboxPollerEngine` listens via `PgNotifyChannel("fyke_inbox_events")`, with safety-net `TimerChannel`.
   - **Poller Refactoring**: Both `OutboxPollerEngine` and `InboxPollerEngine` extend `AbstractPollerEngine`.

7. **Package & Configuration Reorganization**:
   - **Symmetrical Packages**: `dev.fyke.core.outbox` (`OutboxStore`, `JdbcOutboxStore`, `OutboxWriter`, `JdbcOutboxWriter`, `OutboxPollerEngine`) and `dev.fyke.core.inbox` (`InboxStore`, `JdbcInboxStore`, `InboxPollerEngine`, `FykeListener`, `ConsumerPartitionResolver`).
   - **Symmetrical Configuration**: Restructured `FykeProperties` to `fyke.outbox.*` and `fyke.inbox.*` with modular nested `retention: RetentionConfig(enabled, ttl)` (outbox default 7d, inbox default 14d, dlq default 30d) and global cleaner daemon schedule in `fyke.retention`.

8. **Structured Debug & Trace Logging Pipeline**:
   - **Logging Principles**: Codified in `AGENTS.md` (no sensitive payload leakage, clear level separation, zero overhead with parameterized SLF4J formatting).
   - **Granular Visibility**: Added `DEBUG` operational diagnostics (event capture, batch claims with lease duration, dispatch timing, replays, retries, DLQ routing, purge summaries) and `TRACE` mechanics (advisory locks, notifications, payload serialization metrics, headers, reflection dispatch details).
   - **Verification**: Tested with `FykeLoggingTest` verifying log emission and formatting without regressions.

9. **Apache Kafka Binder (`fyke-binder-kafka`) & Documentation Polish**:
   - **Kafka Binder SPI**: Implemented `KafkaBinder : BrokerBinder` using Spring Kafka 4.1.1 with asynchronous `CompletableFuture` producer confirmations.
   - **Consumer Poison Pill DLQ**: Implemented `FykeKafkaDlqRecoverer` capturing consumer exceptions into `fyke_dlq`.
   - **Consumer Ingestion**: Implemented `KafkaConsumerRegistrar` auto-wiring `@FykeListener` Kafka topic consumers with manual immediate offset commit (`AckMode.MANUAL_IMMEDIATE`) and inbox poller wakeup.
   - **Auto-Configuration**: Added Spring Boot 4 auto-configuration support in `fyke-spring-boot-starter` with multi-binder resolution (`fyke.binder=rabbitmq|kafka`).
   - **Testcontainers Verification**: Verified with real Apache Kafka (`apache/kafka:3.7.0` in KRaft mode) and full suite passing.
   - **Documentation**: Updated `README.md` and `CHANGELOG.md` with Transactional Inbox, logging, and Kafka details.

10. **Spring Boot Actuator Health, Management Endpoint & Micrometer Metrics**:
    - **`FykeHealthIndicator`**: Implements Spring Boot 4 `HealthIndicator` (`org.springframework.boot.health.contributor.HealthIndicator`). Provides operational status for `outbox`, `inbox`, and `dlq` with pending/dead counts. Crucially remains `UP` during broker outages to prevent false-positive Kubernetes pod restart cascades.
    - **`FykeEndpoint`**: Implements Spring Boot Actuator `@Endpoint(id = "fyke")` with `@ReadOperation` exposing operational diagnostics (`FykeDiagnosticsSnapshot`), active broker binder, engine settings (batch size, lease duration, concurrency), registered `@FykeListener` instances, active notification channels (`LISTEN/NOTIFY` vs `TIMER`), and live store counters.
    - **`FykeMeterBinder`**: Implements Micrometer's `MeterBinder`, registering gauges for `fyke.outbox.backlog`, `fyke.outbox.dead`, `fyke.inbox.backlog`, `fyke.inbox.dead`, and `fyke.dlq.unreplayed`.
    - **Zero-Overhead CompileOnly**: Actuator and Micrometer dependencies are strictly `compileOnly` in `fyke-spring-boot-starter`. `FykeActuatorAutoConfiguration` gracefully backs off if Actuator/Micrometer classes are absent.
    - **Testcontainers Verification**: Verified against real PostgreSQL 16 and RabbitMQ 3.13 containers via `FykeActuatorIntegrationTest` using `MockMvc`.

---

## 3. Current Status & Git History

All tasks for **P1**, Inbox enhancements, Logging pipeline, Kafka Binder, Actuator Health/Endpoint, and Micrometer Metrics are 100% complete, verified, and passing under `./gradlew check`.

### Verification Command
```bash
./gradlew check
# All 108 actionable tasks pass (Spotless, compilation, unit tests, and Testcontainers integration tests).
```

---

## 4. Exact Next Steps

1. **Benchmarking & High-Concurrency Load Tests**
   - Set up Gatling or JMH benchmarks in `fyke-demo` or a dedicated module.
   - Benchmark throughput with 10,000+ events across multiple concurrent partitions and committers.
   - Measure latency between transaction commit and broker receipt on the `LISTEN`/`NOTIFY` path.

2. **R2DBC / Reactive Seam Evaluation (P1.x)**
   - Evaluate R2DBC support as designed in ADR D-005.
   - Provide a reactive `ReactiveOutboxWriter` implementation using `DatabaseClient` for WebFlux/R2DBC applications.

3. **Prepare for P2 (Remote Control Plane / SaaS Seam)**
   - Verify that the metadata model in `FykeRecordSummary` contains everything needed for the future remote observer/replay relay.
   - Maintain strict air-gap: ensure any future gRPC/HTTP control-plane agent remains a completely optional, separate dependency (`fyke-exporter-controlplane`).
