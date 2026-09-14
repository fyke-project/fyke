# Fyke

**The transactional outbox for Spring Boot that never lets an event escape.**

Fyke is a drop-in starter that makes domain-event delivery *reliable*: your event is written to the database in the **same transaction** as your domain data, delivered **at-least-once** via a pluggable broker, and when something fails you can **see** it, **search** for it, and **replay** it — from a portal, or from within your own JVM.

The name: a *fyke* is a traditional British fish trap — a fixed net with side wings that guide fish in and never let them out. That is exactly what the outbox does for your events.

- **Web:** [fyke.dev](https://fyke.dev)
- **Maven group:** `dev.fyke`

## Status

| Tier | What | Status |
|---|---|---|
| **P1 — Free OSS agent** | in-JVM outbox + poller + DLQ + replay + OTel, zero license | ⬅ in development |
| **P2 — Control plane (hosted SaaS)** | fleet overview, search, alerts, replay relay — metadata-only | planned |
| **P3 — EE (self-hosted license)** | in-VPC control plane: SSO, RBAC, multi-tenant, embed, air-gapped | planned |

## Documentation

- [AGENTS.md](AGENTS.md) — implementation brief (constraints, scope, definition of done)
- [docs/one-pager.md](docs/one-pager.md) — product, problem, tiers, phases
- [docs/requirements-p1.md](docs/requirements-p1.md) — P1 scope + acceptance criteria
- [docs/architecture.md](docs/architecture.md) — component design
- [docs/decisions.md](docs/decisions.md) — every non-obvious choice, with reasoning
- [docs/validation.md](docs/validation.md) — market validation (10 engineer interviews)

## License

Apache 2.0 — see [LICENSE](LICENSE).
