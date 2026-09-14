# One-Pager — Reliable Event-Processing for Spring Boot

> **Name:** **Fyke** *(final — fyke.dev secured. A fyke is a traditional fish trap: fixed in place, side wings guide fish in, never lets them out. That's the outbox, exactly.)*
> **One-liner:** A drop-in **transactional outbox** + **control-plane portal** for Spring Boot apps — your domain events actually land, and when they don't, you can **see, search, and replay** them.

---

## Problem

Every Spring Boot app that publishes domain events (RabbitMQ / Kafka) quietly hits the same three failures:

1. **Lost or duplicate events** — the DB write commits but the publish fails (or a retry double-sends). The classic dual-write problem.
2. **Blind spots** — when something breaks, you don't know what was published, what dead-lettered, or what's stuck in the outbox backlog. You grep logs and pray.
3. **Painful remediation** — replaying or redelivering a message means a JMX console, a hand-rolled script, and a war story.

The *patterns* exist (transactional outbox, DLQ, idempotency) and OSS *libraries* exist — but they're not **productized** into something a Spring dev can wire up in an afternoon and actually *see working*.

---

## What we're building — three tiers

| | **Free OSS agent** (in-JVM, no license) | **Control plane** (hosted SaaS → self-hosted) | **EE** (paid license, ceiling) |
|---|---|---|---|
| **Reliability** | outbox, dedup, DLQ/poison, local trace | fleet health, DLQ view, outbox backlog | air-gapped, audit log |
| **Insight** | local metrics | **message search**, timeline, alerts | long retention |
| **Action** | publish / replay **in-JVM** | **replay relay**, bulk ops | — |
| **Access / UX** | — | basic auth | **SSO, RBAC, multi-tenant, embeddable UI** |
| **Self-serve** | — | — | **config API/UI (no-code webhook facade)** |

**Fixed architecture principle:** the *agent* lives in the app's JVM (it needs the DB + broker binders) and owns all **actions**. Only **observability** is remote. The **control plane's** deployment (SaaS *or* in-VPC) is a free choice — that's where the EU-residency story comes for free. Replay always executes **in-JVM via the agent**; the dashboard just *relays* the command.

---

## Phase model (what we build, in what order)

- **P1 — Free OSS agent (the wedge).** A Spring Boot `@AutoConfiguration` starter: transactional outbox (DB tx → outbox row in the *same tx* → publish via binder; at-least-once + dedup key) with an **efficient poller (LISTEN/NOTIFY + non-locking batch + backoff — not `SELECT…FOR UPDATE`)**, **consumer-side DLQ/poison capture**, a **business-key index** (drives search), **client-side metadata sanitization**, and a local OTel trace. **RabbitMQ first**, Kafka #2. Standalone-useful, **air-gap-first** (no phone-home required), no account, no license. *The discovery asset.* *(Scope below: **Technical requirements**.)*
- **P2 — Hosted SaaS control plane (for reach + first $).** Fleet overview, DLQ view, outbox backlog, message search, timeline, alerts, replay relay (dashboard → agent → broker). **Metadata-only** by default (payloads stay in the customer's DB) → neutralizes most data-residency objections. *Purpose: a linkable 5-min-signup demo, first revenue, and reusable R&D that P3 inherits.*
- **P3 — Self-hosted control plane + paid EE license (the ceiling / EU-residency).** Same control plane in-VPC: SSO/SAML, RBAC, multi-tenancy, embeddable UI, self-service config API/UI (incl. a no-code webhook facade), air-gapped, EU-hosted. **"Datadog, but in our VPC."** Nearly free to add because P2 already built the control plane.

**Why SaaS-first (ramp) then license (ceiling):** with no existing audience, a pure self-hosted license is invisible and slow to sell. The thin SaaS control plane is the *cheapest way to be discovered* and doubles as reusable engineering for the endgame license.

---

## Technical requirements (from the 10 interviews)

The transcripts convert the “1 conditional” into a concrete, scoped backlog. **Bold = day-one P1 must-haves** (named as dealbreakers by people who pay); the rest are deferred *by design*, not by neglect.

| Requirement | Who asked (why) | Where it lands |
|---|---|---|
| **Efficient outbox poller** — LISTEN/NOTIFY + non-locking batch + backoff, *not* `SELECT…FOR UPDATE` | Priya (Aurora I/O bloat), Markus (ShedLock row-lock contention) | **P1 — must** |
| **Consumer-side DLQ / poison capture** (not just publish-side) | Dave (poison pills), Jonas (DLX), Lukas (reboot drops) | **P1 — must** |
| **Business-key index** for search / backlog (`order_id`, `vin_number`, …) | Lukas, Elena, Chloe | **P1 — must** (feeds P2 search) |
| **Client-side metadata sanitization / hashing / field redaction** | Priya, Markus, Chloe (Infosec) | **P1 — must** (makes SaaS safe) |
| **Binder SPI** — abstract *publish* **and** *DLQ/poison* (semantics differ: DLX vs DLT+offsets vs receive-count) | everyone (RabbitMQ / Kafka / SQS / JMS all appear) | **P1 core** (RabbitMQ first, Kafka #2) |
| **DB: Postgres-first** — LISTEN/NOTIFY is a Postgres *wire-protocol* feature → the fast-path wakeup is PG-only; other JDBC DBs get a **timer/safety-net fallback** (interval poller with backoff + dialect-aware batch claim, `SKIP LOCKED` on PG/Oracle) | Priya (Aurora), everyone (all interviewees are on Postgres: Aurora / Heroku / WAL-based) | **P1** — PG is the guaranteed fast path; generic JDBC still works, just not zero-latency |
| **Toolchain** — Spring Boot **4.x only** (no 3.x compat), Kotlin, **Liquibase** (not Flyway), Testcontainers | maintainer | **P1** |
| Selective capture / sampling (critical events vs high-freq telemetry) | Lukas | P1-lite or P2 |
| Multi-squad **standardization**: one starter, one way, documented + replayable | Sarah (6 broken outboxes; “engineer leaves → nobody can replay”) | **P1** — big positioning win |
| Generic **JMS binder** + legacy **JTA/XA** tx managers | Vikram (IBM MQ / ActiveMQ / Tibco) | **P3 / EE** (defer — large surface) |
| Enterprise **SSO** (AD / Keycloak) + **RBAC** for non-engineers | Vikram, Sarah | **EE (P3)** |
| Embeddable UI / self-service config | Sarah, Chloe | **EE (P3)** |

**The deferrals are strategic:** JMS / JTA / SSO / embed are the *enterprise & regulated ceiling* (P3/EE) — they’re what Vikram and Sarah need to *buy*, but they are **not** day-1 blockers for the P1 wedge.

---

## P2 — Control/data channel *(the metadata-only question, answered)*

How a **metadata-only** SaaS does search + replay without ever holding payloads:

**Replay = command relay (your instinct, confirmed).** The control plane never needs the *payload* to trigger a replay — only the event’s **identity** (outbox row id / business key / seq) + the instruction. The command rides **down** to the agent, which (in-JVM, with the DB + binder) re-publishes the locally-stored row. For a DLQ’d message, the **agent** already holds the payload in its local DLQ table; the SaaS only has *metadata about* it. Bolt on three things: **authenticity** (agent verifies the command is signed by the legit control plane), **authorization** (who may issue which command → EE RBAC), and **audit** (who / when / which key → the audit log Markus & Chloe require).

**Search: metadata is *enough* for the SaaS tier — by design.** Every named use case searches by **business key + status + type + time**, never free-text content: Elena (“Tenant X didn’t get an invite”), Lukas (“backlog by `vin_number`/`device_id`”), Jonas (“which order failed in the DLX”), Chloe/Markus (ledger reconciliation by sequence). So the metadata we ship = business key(s), event type, status (pending / published / consumed / dead), timestamps, consumer, correlation/trace id, size.

**Content search is a self-hosted/EE feature** — and the metadata-only constraint *creates* that tier boundary for us:
- **SaaS:** metadata only (payloads never leave the VPC → GDPR / Infosec-safe — exactly what Markus, Chloe, Priya require).
- **Self-hosted EE (in-VPC):** the control plane *can* fetch content on demand, because it’s inside their boundary. “Search the actual payload” exists, but only where it’s allowed to exist. → **One search UI, two data sources** (metadata always; content only in self-hosted mode).

**gRPC streaming: yes — one agent-initiated bidirectional stream, three flows.** The linchpin: **the agent opens the outbound connection** (agent = client). That (a) works in **egress-only** VPC firewalls (no inbound ports) → why it’s friendly to regulated buyers, and (b) means **one agent build** works for SaaS (point at our endpoint) and self-hosted (point at their in-VPC endpoint). Over that single persistent stream:
1. **up — telemetry:** **reuse OTLP** (the agent’s OTel spans + custom attrs ship as standard OTLP/gRPC; self-hosted can route via their OTel Collector). We don’t invent a telemetry protocol.
2. **down — commands:** replay / ack / purge (authenticated + authorized).
3. **up (on-demand) — content fetch:** **enabled only in self-hosted mode**; in SaaS mode the agent is configured to never send payloads (client-side sanitization, per above).

**Graceful degradation (a real selling point):** if the stream drops, *remote* search/trigger pauses — but the agent’s **local** reliability (outbox, DLQ, local trace) keeps running 100%. The agent is the source of truth for *reliability*; the stream is only for *remote insight + remote triggers*. So an air-gapped / regulated customer who *never* connects the agent to a SaaS still gets all of P1. → This *is* the air-gap-first mandate, made concrete.

---

## Positioning — and where we draw the lines

| Player | What they are | Where we're different |
|---|---|---|
| **Datadog** | generic APM / observability | we're *messaging-domain-specialized* (outbox, DLQ, replay) |
| **Hookdeck** | webhook infrastructure | we're *outbox / domain events*; webhook is a **door**, not the headline |
| **Debezium** | CDC — *any DB change → broker* (infra-level, DB-agnostic) | we're *app-level Spring domain events* |
| **Axon** | heavyweight event-sourcing framework | we're the *lightweight drop-in* |
| **OSS libs** | copyable pattern code | we're **productized**: multi-binder + portal + in-JVM actions + license |

---

## Moat (the honest version)

The outbox *pattern* is textbook — not ownable. What we actually own is **productization** (multi-binder abstraction + portal + in-JVM actions + license) and **audience / portfolio lock-in**. There is **no hard technical moat** — so we **broaden scope over time** (reliability → more of the messaging lifecycle) and build the portfolio/audience asset **early**. That *is* the long-term defensibility.

---

## Pricing sketch *(directional — validate in interviews, not a promise)*

| Tier | What | Price (sketch) |
|---|---|---|
| **Free OSS agent** | full in-JVM reliability, no license | $0 |
| **SaaS — Hobby** | 1 app, 7-day retention, community | $0 |
| **SaaS — Starter** | 5 apps, 30-day retention, search | ~€50/mo |
| **SaaS — Growth** | 20 apps, 90-day, alerts + replay relay | ~€250/mo |
| **SaaS — Scale** | unlimited apps, 1-yr retention, SSO/RBAC, SLA | ~€1,000/mo (custom) |
| **EE (self-hosted)** | in-VPC: SSO, RBAC, multi-tenant, embed, air-gapped, EU-resident | ~€1.5k–5k/yr per cluster (annual) |

---

## Validation plan

- **8–10 conversations** with Spring / MQ engineers. **Success = 6+ say yes.**
- **Killer probe** (budget proxy that predicts willingness-to-pay): *"What do you currently pay for around messaging — and what breaks right now?"*

**Interview questions:**

1. *"What do you currently **pay for** around messaging — brokers, observability, dead-letter handling? And what **breaks**?"*
2. *"When a message silently fails to publish, or a consumer dies, how do you find out — and how long does it take? What does that cost you?"*
3. *"If you could stand up a portal that shows your outbox backlog + DLQ and lets you **search and replay** a message — what would that save you? Who on your team would actually use it?"*
4. *"How does your **data-residency / compliance** posture change your willingness to use a hosted version vs. self-hosted?"*
5. *"Who else in your org would care about this, and what **budget line** would it sit under?"*

---

## Validation results (Sep 2026) — **PASS**

| Metric | Result | Read |
|---|---|---|
| **Validation rate** (threshold 6+) | **7 / 10 Yes** (3 EE, 3 SaaS, 1 conditional) | **Above threshold.** Strong resonance in mid-market B2B + regulated industries. |
| **OSS wedge** (outbox starter) | **10 / 10** | Unanimous. The outbox is universally painful to maintain in-house → the right wedge. |
| **Deployment split** | 4 SaaS (mid-market) · 4 self-hosted (fintech / health / insurtech) · 2 OSS-only | **Directly validates the bifurcated model:** SaaS = fast cash from growth B2B; EE/self-hosted = high-ticket, best-fit for regulated EU. |

**Strategic read:**
- The **regulated EU segment** (fintech, health, insurtech) is the highest-ticket *and* the best strategic fit — they demand in-VPC, which is exactly the data-residency differentiator. → **The P1 agent must be air-gap-first / fully self-contained** (the SaaS control plane is an *optional* exporter, never a requirement). Regulated buyers will reject an agent that phones home.
- The **1 conditional** is no longer a loose thread — their conditions (efficient poller, sampling, JMS/JTA/SSO) are captured in the **Technical requirements** table (above). All buildable; **none block P1**.

---

## Open items

- **Name:** **Fyke** — final (fyke.dev). Repo: `~/dev/fyke`, Maven group `dev.fyke`.
- **Impl brief:** agent-ready doc skeleton lives in `~/dev/fyke` (`AGENTS.md` + `docs/`) — no code yet; hand off to the implementation agent there.
- **Binder order:** RabbitMQ → Kafka → SQS / PubSub. *(Interviews: Kafka ≈ RabbitMQ in headcount. Holding **RabbitMQ-first** for the cleanest DLX demo + your comfort zone — the **binder SPI** is what makes the rest a non-issue.)*
- **Next step:** **build P1** (the free OSS outbox agent) — validation passed; the 10/10 wedge is the discovery asset both SaaS and EE stand on. Implementation hands off via the `~/dev/fyke` brief.
