# AGENTS.md — Implementation brief for Fyke

You are building **Fyke** — a transactional outbox + reliability agent for Spring Boot, in Kotlin. Read the docs below **before writing any code**.

> Tooling note: this brief follows the `AGENTS.md` convention. If your agent tooling expects a different filename (e.g. `CLAUDE.md`, `.cursor/rules`), copy this file — do not maintain two versions.

## What this is

A Spring Boot starter that guarantees domain events actually land in the broker, and lets you see, search, and replay the ones that didn't. Later phases add a control-plane portal (hosted SaaS, then self-hosted EE license) on top of the same agent. Product story: [docs/one-pager.md](docs/one-pager.md).

## Current task: implement P1 (the free OSS agent)

- Full scope + acceptance criteria: [docs/requirements-p1.md](docs/requirements-p1.md)
- Component design: [docs/architecture.md](docs/architecture.md)
- Why every choice was made: [docs/decisions.md](docs/decisions.md)
- Market evidence (10/10 interviews = yes): [docs/validation.md](docs/validation.md)

## Hard constraints — do not violate (each has a "why" in decisions.md)

1. **Air-gap-first.** The agent must be 100% functional with *zero* external connections. There is no control plane, no phone-home, no telemetry by default. Any remote integration is an *optional exporter the user configures*.
2. **Actions in-JVM.** All mutating operations (publish, replay, ack, purge) execute inside the app's JVM via the agent. Anything remote only *observes* and *relays commands* (and that's P2, not P1).
3. **Spring Boot 4.x only.** Target the latest 4.x stable. Do **not** build a 3.x compatibility path.
4. **Kotlin** on JVM 21.
5. **Postgres-first.** The zero-latency path (LISTEN/NOTIFY + `SKIP LOCKED`) is Postgres. Other JDBC DBs get a timer-based fallback — supported, but Postgres is the only *guaranteed, fully tested* path.
6. **Liquibase**, not Flyway, for schema migrations. The changelog ships inside the starter JAR.
7. **RabbitMQ is the first binder.** The binder is an SPI — core must never reference a concrete broker. Kafka comes *after* the P1 demo is green.
8. **Same-transaction capture.** The outbox row is written in the *same DB transaction* as the domain data. Non-negotiable — this is the entire point.
9. **No secrets, no external services** in the repo or its tests (Testcontainers only).

## Quality bar

- Integration tests use **Testcontainers** (Postgres + RabbitMQ). Never mock the broker for a reliability scenario.
- Every acceptance criterion in requirements-p1.md has a test that would fail if the behavior regressed.
- The demo app must prove, in CI: (1) kill JVM mid-transaction → no lost event; (2) broker down → backlog drains after recovery, no duplicates; (3) poison message → DLQ'd, searchable by business key, replayable in-JVM.
- KDoc on all public API. The README quickstart must work copy-paste.

## Out of scope — do NOT build (these are later phases)

- P2: gRPC channel, control-plane client, dashboard UI, hosted SaaS.
- P3/EE: JMS/JTA/XA binders, SSO/RBAC, multi-tenancy, embeddable UI.
- Reactive/R2DBC capture: design the **seam** (decisions.md D-005), do **not** implement it.
- Kafka binder (second binder — only after P1 demo is green).
- Publishing to Maven Central — only when the maintainer asks.

## Definition of done (P1)

1. All acceptance criteria in requirements-p1.md pass under `./gradlew build` (Testcontainers).
2. Expected module layout: `fyke-core`, `fyke-binder-rabbitmq`, `fyke-spring-boot-starter`, `fyke-demo` (Gradle multi-module, Kotlin DSL).
3. Zero-config starter: a stock Boot app with `fyke-spring-boot-starter` + DataSource + Spring AMQP works with no additional configuration.
4. README quickstart + CHANGELOG present. Nothing in the repo phones home.

## Verify before scaffolding

These coordinates rot — check them against the official Spring docs / Maven Central, don't trust stale snippets:

- Latest **Spring Boot 4.x** patch version (start.spring.io is the reference).
- **Boot 4 reorganized its module/starter layout** — confirm the current artifact coordinates for JDBC, AMQP, OTel, Liquibase before pinning them.
- Kotlin + KGP version compatible with Boot 4; Spring AMQP version pairing with Boot 4.
- Testcontainers image tags: Postgres ≥ 16 (transactional NOTIFY needs ≥ 14), RabbitMQ stable.
