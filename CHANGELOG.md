# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0-SNAPSHOT] - 2026-09-14

### Added
- **Core Engine (`fyke-core`)**:
  - Embedded Liquibase database changelog creating `fyke_outbox` and `fyke_dlq` tables with partition and status indexes.
  - Transaction-bound outbox capture (`OutboxWriter`) guaranteeing domain write and event write commit atomically.
  - Dual-mode notification poller (`NotificationSource`): zero-latency PostgreSQL `LISTEN`/`NOTIFY` (`PgNotifyChannel`) and periodic fallback timer (`TimerChannel`).
  - Strict FIFO partition processing with PostgreSQL advisory locks (`PostgresAdvisoryPartitionLocker`) and row-level `FOR UPDATE SKIP LOCKED`.
  - Configurable partitioning strategies (`PartitionResolver`) with single global partition and business-key partitions.
  - Dead Letter Queue (`fyke_dlq`) supporting both publisher failures and consumer poison pills with original message bodies and error stack traces.
  - In-JVM event replay (`PollerEngine.replay`) re-delivering directly through the configured broker binder.
  - Unified search by business key (`OutboxStore.searchByBusinessKey`) across outbox and DLQ tables.
  - Automatic retention cleaner (`RetentionCleaner`) purging aged published and replayed records in batches.
  - OpenTelemetry integration (`FykeTelemetry`) with client-side header sanitization (`ClientSideSanitizer`).
- **RabbitMQ Binder (`fyke-binder-rabbitmq`)**:
  - `RabbitBinder` implementing `BrokerBinder` with Spring AMQP and publisher confirms (`CorrelationData`).
  - `FykeRabbitDlqRecoverer` implementing Spring AMQP `MessageRecoverer` for dead-lettering consumer failures into `fyke_dlq`.
  - `RabbitConsumerRegistrar` auto-discovering `@FykeListener` beans and ingesting incoming messages to `fyke_inbox` with immediate broker ACK and environment placeholder resolution.
- **Kafka Binder (`fyke-binder-kafka`)**:
  - `KafkaBinder` implementing `BrokerBinder` using Spring Kafka 4.1+ with asynchronous publisher confirmations.
  - `FykeKafkaDlqRecoverer` capturing poison-pill consumer exceptions into `fyke_dlq`.
  - `KafkaConsumerRegistrar` dynamically registering `@FykeListener` Kafka topic containers with manual immediate offset commit (`AckMode.MANUAL_IMMEDIATE`) and placeholder resolution.
- **Transactional Inbox & Consumer Ordering**:
  - `02-create-inbox-table.sql` Liquibase changelog creating `fyke_inbox` table and indexes.
  - `@FykeListener` annotation for declarative event consumption with configurable ordering modes (`STRICT_FIFO`, `LEAPFROG`).
  - `InboxPollerEngine` and `JdbcInboxStore` supporting per-partition mutual exclusion, exponential retry backoff, and instantaneous unblocking via `Fyke.retryInbox(id)`.
  - Instant fatal poison-pill fast-path directly to `fyke_dlq` without retry delay.
  - Symmetrical `AbstractPollerEngine` base class orchestrating notification sources, leases, backoff, and concurrency.
  - Zero-latency inbox notification path via PostgreSQL `LISTEN`/`NOTIFY` on `fyke_inbox_events`.
- **Spring Boot Starter (`fyke-spring-boot-starter`)**:
  - Zero-configuration auto-configuration (`FykeAutoConfiguration`) for Spring Boot 4.1+.
  - Static facade `Fyke` (`send`, `replay` / `replayOutbox`, `retryInbox`, `searchByBusinessKey`).
  - Symmetrical configuration under `fyke.outbox.*` and `fyke.inbox.*` with modular retention configurations.
  - Annotation-driven capture via `@FykeEvent` and Spring's `ApplicationEventPublisher`.
  - Auto-configuration ordering after Boot's `DataSourceAutoConfiguration`, `RabbitAutoConfiguration`, and `KafkaAutoConfiguration`.
- **Spring Boot Actuator & Micrometer Metrics (`fyke-spring-boot-starter`)**:
  - `FykeHealthIndicator`: Spring Boot 4 `HealthIndicator` reporting symmetrical `outbox`, `inbox`, and `dlq` operational status; remains resilient (`UP`) during broker outages to prevent false-positive Kubernetes pod restarts.
  - `FykeEndpoint`: Dedicated read-only management endpoint (`/actuator/fyke`) exposing operational diagnostics (broker binder, batch size, lease duration, concurrency, active notification sources, registered listeners, and store counts).
  - `FykeMeterBinder`: Micrometer metrics binder registering gauges for `fyke.outbox.backlog`, `fyke.outbox.dead`, `fyke.inbox.backlog`, `fyke.inbox.dead`, and `fyke.dlq.unreplayed`.
  - `FykeActuatorAutoConfiguration`: Conditional auto-configuration backing off gracefully if Actuator or Micrometer are not present.
- **Observability & Logging**:
  - Structured `DEBUG` operational diagnostics (batch claims, publish timings, DLQ routes) and `TRACE` mechanics (advisory locks, notifications, payload sizes, headers).
  - Strict zero payload leakage policy across `DEBUG` and `INFO` levels.
  - Symmetrical naming across notification channels (`fyke_outbox_events` vs `fyke_inbox_events`) and timer threads (`fyke-outbox-timer` vs `fyke-inbox-timer`).
- **End-to-End Demo & Test Suite (`fyke-demo`)**:
  - Docker Compose setup (`docker-compose.yml`) for local testing with PostgreSQL 17, RabbitMQ 4, and Redpanda.
  - Profile-specific configurations for RabbitMQ (`application-rabbitmq.yml`) and Kafka (`application-kafka.yml`).
  - Testcontainers verification with real PostgreSQL 16, RabbitMQ 3.13, and Apache Kafka containers.
  - Verified scenarios:
    1. Pod death mid-transaction recovery (outbox orphaned row claimed and published).
    2. Atomic commit and zero-latency delivery.
    3. Idempotency deduplication by key.
    4. Broker recovery after outage draining backlog without duplicates.
    5. Consumer poison pill capture to `fyke_dlq` and in-JVM replay.
    6. Transaction rollback discarding outbox records.
    7. Transactional Inbox end-to-end receipt via `@FykeListener`.
    8. Transactional Inbox strict per-partition FIFO with retry backoff and unblock.
    9. Transactional Inbox fatal poison pill fast-paths directly to DLQ.
    10. Producer outbox prevents leapfrogging when earlier partition record is in retry backoff.
    11. Apache Kafka binder end-to-end delivery and poison-pill DLQ recovery (`FykeKafkaScenariosTest`).
    12. Actuator health indicators, `/actuator/fyke` endpoint, and Micrometer metrics (`FykeActuatorIntegrationTest`).
