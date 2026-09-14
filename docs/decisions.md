# Decisions (ADRs)

Short form: **context → decision → consequences**. The long rationale (interviews, market logic) is in [one-pager.md](one-pager.md) and [validation.md](validation.md).

---

## D-001 — All actions execute in-JVM

**Context.** The outbox agent lives in the app's JVM because it needs the DB and the broker binders. A remote system that can *mutate* messaging state (replay, purge, ack) would be a security incident waiting to happen — and the first thing an Infosec team would block (interviews: Markus, Chloe).

**Decision.** Every mutating operation (publish, replay, ack, purge) executes inside the app via the agent. Anything remote only *observes* and *relays signed commands* (P2, not P1).

**Consequences.** Replay is "safe" for non-engineers to trigger from a UI later, because the blast radius is the app's own JVM with its own credentials. P2 must still add command signing + authorization + audit.

## D-002 — Air-gap-first

**Context.** The best-fit regulated buyers (fintech/health/insurtech, 4 of 10 interviews) reject any agent that phones home. An agent that *requires* a cloud connection is unsellable to exactly the segment that pays most.

**Decision.** P1 is 100% functional with zero external connections. No phone-home, no telemetry by default. Remote integration = optional, user-configured exporter (P2).

**Consequences.** P1 has no network surfaces at all — simpler to write, simpler to pass Infosec. P2's gRPC stream is purely additive.

## D-003 — Binder SPI, RabbitMQ first

**Context.** Every interviewee is on a different broker (RabbitMQ, Kafka/MSK, SQS/SNS, IBM MQ, ActiveMQ, Tibco). Hard-coding one broker kills the abstraction. But we must ship *something*.

**Decision.** Core speaks a 4-op binder SPI (`publish`/`ack`/`purge`/DLQ-attach); core never touches a concrete broker. **RabbitMQ is the first binder**: cleanest DLX demo story, maintainer's comfort zone, and the binder SPI makes Kafka a later module, not a redesign.

**Consequences.** DLQ semantics differ per broker (DLX vs dead-letter topic + offsets vs receive counts) — the SPI's `DLQ-attach` abstraction must be honest about those differences, not pretend uniformity. JMS/JTA binders deferred to EE (D-scope, interview: Vikram).

## D-004 — Postgres-first database tier

**Context.** The zero-latency poller path (R2) leans on `LISTEN/NOTIFY` — a **Postgres wire-protocol feature**, not SQL. Priya's Aurora pain (I/O bloat from 1s polling) is the dealbreaker we're fixing. All 10 interviewees run Postgres in some form (Aurora, Heroku, self-hosted).

**Decision.** Postgres ≥ 14 (transactional NOTIFY) is the *guaranteed, fully tested* path; other JDBC DBs get the `TimerChannel` fallback (interval + backoff + dialect-aware `SKIP LOCKED` claim) — supported, but explicitly not the zero-latency path.

**Consequences:** the DB is **required to be Postgres for the fast path** (not "any JDBC connection will do") — this must be stated plainly in the README. Non-PG users get a working but slower agent. A PG-specific wire connection (separate from the app's JDBC pool) is required for LISTEN.

## D-005 — JDBC-first capture; reactive as a later seam

**Context.** The maintainer works daily with R2DBC/Reactor and asked whether a reactive version is worth it. But the load-bearing constraint is that the outbox row **joins the domain's open transaction** — in a R2DBC/Reactor app that transaction is a `Mono`/`Flux` publisher, not a `Connection` in a thread.

**Decision.** P1 implements the synchronous JDBC capture path only. But the abstraction boundary is **`OutboxWriter`** (transaction-local write) — not "JDBC". Everything downstream of `OutboxStore` (poller claim SQL, binder, DLQ) is storage-agnostic and shares code. An R2DBC `OutboxWriter` implementation slots in as **P1.x** without touching the rest.

**Consequences.** We don't double the P1 surface for a minority segment before the wedge is proven. If early demand for Reactor appears, it's one module, not a rewrite. The `OutboxWriter` seam must be designed *now* even though only one impl ships.

## D-006 — Spring Boot 4.x only, no 3.x compatibility

**Context.** Start.spring.io no longer offers Boot 3.x; latest stable is 4.x. Supporting two majors roughly doubles the maintenance surface (CI matrix, dependency conflicts, test runs) for a 6–8 h/week side project.

**Decision.** Target the latest Boot 4.x stable. **No 3.x line.**

**Consequences.** The wedge is *new adoption*: discovery users (blog readers, OSS adopters) run current Boot anyway. Existing 3.x shops maintain home-grown outbox code and are unlikely to adopt a *new* library mid-support-cycle — they'll come at upgrade time. Spring supports 3.x for a while, so no one is stranded. **Revisit only if early demand for 3.x appears** (watch issue tracker after launch).

## D-007 — Liquibase, not Flyway

**Context.** Maintainer preference, and both are viable. Flyway is marginally more popular in the Boot world, but the decision criteria (changelog ships in the JAR, declarative SQL/YAML, zero config) are met equally by Liquibase, and the maintainer will maintain this.

**Decision.** Liquibase for all schema migrations; the changelog bundles inside the starter JAR and auto-applies.

**Consequences.** Users on Flyway-only setups need `spring.liquibase.enabled=true` (Boot default) — README notes the coexistence is fine (separate namespace). One migration system per project rule applies at the user's discretion; we don't try to support Flyway-side application.

## D-008 — Kotlin on JVM 21

**Context.** Maintainer's language of choice; Spring Boot 4 requires JDK 17+ and targets modern runtimes. Kotlin gives idiomatically clean SPI + property-binding code.

**Decision.** Kotlin (KGP, stable release) targeting JVM 21 bytecode.

**Consequences.** Binary/API consumers in Java are fine (Kotlin is consumable from Java). KDoc on all public API keeps the Java-side story clean.

## D-009 — Metadata-only SaaS boundary

**Context.** Regulated interviewees (Markus, Chloe, Priya) will block a hosted UI that sees payment/medical/transaction payloads. Yet the SaaS tier is the discovery + first-revenue vehicle.

**Decision.** The data boundary is enforced **client-side in the agent** (R6 sanitization): the SaaS control plane receives **metadata only** (business keys, types, statuses, timestamps, sizes, hashes) — never raw payloads. Self-hosted EE can fetch content on demand *inside the customer's boundary*. One search UI, two data sources.

**Consequences.** This constraint *creates* the SaaS/EE tier line — the same feature set, different data access. "Search payload content" is an honest EE differentiator, not a lock-in trick. The agent must be configured *per-deployment* for what it may send; the default (SaaS mode) never sends payloads.

## D-010 — Apache-2.0

**Context.** OSS wedge strategy; must be safe for the enterprise/regulated buyers to *adopt the agent* without legal friction, while leaving room for the paid control plane.

**Decision.** Agent = Apache-2.0. The control plane (P2/P3) is a separate, closed-source product; the agent's open code never contains control-plane code.

**Consequences.** Patents grant, no copyleft — maximally permissive for adoption. The SaaS/EE moat is *product surface + license on the control plane*, not on the agent.

## D-011 — Name: Fyke (fyke.dev)

**Context.** Candidate names from the "fixed structure that captures flow" metaphor family: keystone, sluice, catchment, fyke. `fyke` is a traditional British bag-style fish trap: fixed in place, side wings guide fish *in*, never lets them out. The name converged independently — it was chosen before this project and the domain was already secured.

**Decision.** **Fyke.** Domain `fyke.dev`, Maven group `dev.fyke`.

**Consequences.** Distinctive, short, memorable, zero trademark collision (Hookdeck's webhook-sending product is "Outpost" — we explicitly are not). The name *is* the product metaphor: events guided in, never escaping — which is the README's founding story.
