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
- **Spring Boot Starter (`fyke-spring-boot-starter`)**:
  - Zero-configuration auto-configuration (`FykeAutoConfiguration`) for Spring Boot 4.1+.
  - Static facade `Fyke` (`send`, `replay`, `searchByBusinessKey`).
  - Annotation-driven capture via `@FykeEvent` and Spring's `ApplicationEventPublisher`.
  - Auto-configuration ordering after Boot's `DataSourceAutoConfiguration` and `RabbitAutoConfiguration`.
- **End-to-End Demo & Test Suite (`fyke-demo`)**:
  - Testcontainers verification with real PostgreSQL 16 and RabbitMQ 3.13 containers.
  - Verified scenarios:
    1. Transaction rollback discarding outbox records.
    2. Atomic commit and zero-latency delivery.
    3. Idempotency deduplication by key.
    4. Broker recovery after outage draining backlog without duplicates.
    5. Consumer poison pill capture to `fyke_dlq` and in-JVM replay.
