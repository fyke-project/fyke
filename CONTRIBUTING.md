# Contributing to Fyke

Thank you for your interest in contributing! This document explains how to get
the project running locally and what we expect from pull requests.

## Prerequisites

| Tool | Required version |
|---|---|
| JDK | 21+ (Temurin recommended) |
| Docker | 24+ (Testcontainers uses the Docker socket) |
| Git | any recent version |

Gradle itself is managed by the included wrapper (`./gradlew`) — no local
installation needed.

## Getting started

```bash
git clone https://github.com/fyke-project/fyke.git
cd fyke
./gradlew assemble   # compile all modules
./gradlew check      # compile + format-check + test (requires Docker)
```

Integration tests spin up real **PostgreSQL 16** and **RabbitMQ** containers
automatically via Testcontainers. No manual container management needed.

## Code style

Fyke uses [Spotless](https://github.com/diffplug/spotless) for formatting.
Before committing, run:

```bash
./gradlew spotlessApply   # auto-fix formatting
./gradlew spotlessCheck   # verify (what CI runs)
```

The rules in brief: tabs for Kotlin/Gradle, 2-space indent for YAML/Markdown,
trailing whitespace trimmed, files end with a newline.

## Project layout

```
fyke-core/               Core domain: stores, pollers, partitioning, retention
fyke-binder-rabbitmq/    Spring AMQP binder with publisher confirms
fyke-binder-kafka/       Spring Kafka binder
fyke-spring-boot-starter/ Auto-configuration, Fyke facade, Actuator, Micrometer
fyke-demo/               Runnable demo + Testcontainers acceptance tests
fyke-benchmarks/         High-concurrency latency benchmarks (not in standard CI)
docs/                    Architecture, requirements, decisions, validation
```

## Running the demo app

```bash
# Start dependencies (Postgres + RabbitMQ + Kafka)
docker compose up -d

# Run the demo
./gradlew :fyke-demo:bootRun
```

## Benchmarks

The benchmark suite is excluded from standard CI because it needs dedicated
hardware for meaningful numbers. Run it locally with:

```bash
./gradlew :fyke-benchmarks:benchmark
```

## Submitting a pull request

1. Fork the repo and create a feature branch off `main`.
2. Keep commits focused — one logical change per commit, conventional-commit
   style messages (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`).
3. Add or update tests for every behavior change. Integration tests must use
   Testcontainers — do not mock the broker for reliability scenarios.
4. Ensure `./gradlew check` passes locally before pushing.
5. Open the PR against `main` and fill in the template.

## Hard constraints (please read before proposing changes)

These constraints come from [AGENTS.md](AGENTS.md) and [docs/decisions.md](docs/decisions.md).
Proposals that violate them will not be merged:

- **Air-gap first** — no phone-home, telemetry, or external control plane.
- **Same-transaction capture** — outbox row must be written in the same DB
  transaction as the domain data.
- **Spring Boot 4.x only** — no 3.x compatibility path.
- **Liquibase** (not Flyway) for schema migrations.
- **Inbox/Outbox naming symmetry** — always explicit, never implicit.

## Reporting bugs & feature requests

Please use the GitHub issue templates. For security vulnerabilities, do **not**
open a public issue — email the maintainer directly instead.

## License

By contributing you agree that your contributions will be licensed under the
[Apache 2.0 License](LICENSE).
