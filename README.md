# Fyke

**The transactional outbox for Spring Boot that never lets an event escape.**

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1+-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![JVM](https://img.shields.io/badge/JVM-21+-orange.svg)](https://openjdk.org/)
[![Kotlin](https://img.shields.io/badge/Kotlin-2.3+-purple.svg)](https://kotlinlang.org/)

Fyke is a drop-in starter that guarantees domain event delivery is **atomic, reliable, and recoverable**:
- **Same-transaction capture**: Events are persisted to the database in the exact same SQL transaction as your business data.
- **Zero-latency dispatch**: Dispatched to your broker using PostgreSQL `LISTEN`/`NOTIFY` combined with `SKIP LOCKED` and advisory locks.
- **Pluggable broker binders**: Pluggable SPI with out-of-the-box RabbitMQ support (publisher confirms enabled).
- **In-JVM search & replay**: Dead-lettered events (publisher exhaust or consumer poison pills) can be found by business key and replayed directly inside your running JVM.
- **Air-gap first**: Zero external control plane, phone-home, or third-party cloud requirements.

---

## Architecture at a Glance

```
Application Transaction
┌────────────────────────────────────────────────────────┐
│  @Transactional fun placeOrder() {                     │
│      orderRepository.save(order)                       │
│      fyke.send(event)  ──► writes to fyke_outbox       │
│  }                     ──► pg_notify('fyke_outbox')    │
└────────────────────────────────────────────────────────┘
                       │ COMMIT
                       ▼
Postgres LISTEN / NOTIFY Channel
                       │ Wakeup
                       ▼
Fyke Poller Engine (Advisory Lock per Partition)
                       │ SELECT ... FOR UPDATE SKIP LOCKED
                       ▼
Broker Binder (RabbitMQ / Kafka) ──► Publisher ACK Confirm
                       │
       ┌───────────────┴───────────────┐
       ▼ ACK                           ▼ NACK / Exhaust
Status: PUBLISHED               fyke_dlq (Reason + Trace)
                                       │
                                       ▼ Fyke.replay(id)
```

---

## Quickstart

### 1. Add Dependencies

In your Spring Boot 4 application's `build.gradle.kts`:

```kotlin
dependencies {
    implementation("dev.fyke:fyke-spring-boot-starter:0.1.0-SNAPSHOT")
    implementation("dev.fyke:fyke-binder-rabbitmq:0.1.0-SNAPSHOT")
    implementation("org.springframework.boot:spring-boot-starter-amqp")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    runtimeOnly("org.postgresql:postgresql")
}
```

### 2. Configure Database & Broker

Fyke works out of the box with zero custom configuration when PostgreSQL and RabbitMQ are available. Fyke's bundled Liquibase changelog automatically creates the `fyke_outbox` and `fyke_dlq` tables.

Optional configuration in `application.yml`:

```yaml
fyke:
  enabled: true
  outbox:
    channel: auto          # AUTO (uses PG_NOTIFY on Postgres), PG_NOTIFY, or TIMER
    batch-size: 50
    lease-duration: 30s
    max-attempts: 5
    poll-interval: 1000ms  # Fallback safety timer
    concurrency: 1
    retention:
      enabled: true
      ttl: 7d              # Delete published outbox records after 7 days
  inbox:
    channel: auto
    batch-size: 50
    lease-duration: 30s
    max-attempts: 5
    poll-interval: 500ms
    concurrency: 4
    retention:
      enabled: true
      ttl: 14d             # At-least-once deduplication window
  dlq:
    retention:
      enabled: true
      ttl: 30d             # Retain DLQ records for incident triage
  retention:
    enabled: true          # Cleaner daemon master switch
    purge-interval: 1h     # Run retention cleaner every hour
    batch-size: 1000
  rabbitmq:
    confirm-timeout: 5s    # Publisher confirm timeout
```

### 3. Publishing Events

#### Option A: Annotation-driven via Spring Application Events

Annotate your event class with `@FykeEvent` and publish via standard Spring `ApplicationEventPublisher`:

```kotlin
import dev.fyke.starter.annotation.FykeEvent

@FykeEvent(
    destination = "orders.exchange",
    target = "order.created",
    type = "OrderCreatedEvent",
    businessKeyProperty = "orderId"
)
data class OrderCreatedEvent(
    val orderId: String,
    val amount: BigDecimal
)
```

```kotlin
@Service
class OrderService(
    private val orderRepository: OrderRepository,
    private val eventPublisher: ApplicationEventPublisher
) {
    @Transactional
    fun placeOrder(orderId: String, amount: BigDecimal) {
        orderRepository.save(Order(orderId, amount))
        // Captured in the exact same database transaction:
        eventPublisher.publishEvent(OrderCreatedEvent(orderId, amount))
    }
}
```

#### Option B: Programmatic Capture via `Fyke.send(...)`

```kotlin
import dev.fyke.starter.Fyke

@Transactional
fun placeOrder(orderId: String, amount: BigDecimal) {
    orderRepository.save(Order(orderId, amount))

    Fyke.send(
        type = "OrderCreated",
        destination = "orders.exchange",
        target = "order.created",
        businessKey = orderId,
        payload = mapOf("orderId" to orderId, "amount" to amount)
    )
}
```

---

## Consumer Poison-Pill DLQ Protection

Fyke captures poison pills from failing message consumers directly into `fyke_dlq`:

```kotlin
@Configuration
class RabbitConsumerConfig {
    @Bean
    fun rabbitListenerContainerFactory(
        connectionFactory: ConnectionFactory,
        dlqRecoverer: FykeRabbitDlqRecoverer
    ): SimpleRabbitListenerContainerFactory {
        val factory = SimpleRabbitListenerContainerFactory()
        factory.setConnectionFactory(connectionFactory)
        factory.setMessageConverter(Jackson2JsonMessageConverter())
        
        // Retry 3 times, then route directly to fyke_dlq:
        val retryInterceptor = RetryInterceptorBuilder.stateless()
            .maxAttempts(3)
            .recoverer(dlqRecoverer)
            .build()
        factory.setAdviceChain(retryInterceptor)
        return factory
    }
}
```

---

## In-JVM Search & Replay

When events fail or get captured in the DLQ, investigate and replay them without leaving the JVM:

```kotlin
import dev.fyke.starter.Fyke

// 1. Search outbox and DLQ records by business key (e.g. orderId)
val records = Fyke.searchByBusinessKey("order-12345")
records.forEach { record ->
    println("ID: ${record.id} [${record.status}] source=${record.source} error=${record.reason}")
}

// 2. Replay a dead-lettered message directly to the broker
val replayed = Fyke.replay(recordId)
if (replayed) {
    println("Event successfully re-published to broker!")
}
```

---

## Modules

| Module | Description |
|---|---|
| `fyke-core` | Core domain models, Liquibase changelog, `OutboxStore`, `InboxStore`, `OutboxPollerEngine`, `InboxPollerEngine`, partitioning, retention cleaner, and OpenTelemetry. |
| `fyke-binder-rabbitmq` | Spring AMQP RabbitMQ binder with publisher confirms and `FykeRabbitDlqRecoverer`. |
| `fyke-spring-boot-starter` | Spring Boot 4 auto-configuration and `Fyke` static facade. |
| `fyke-demo` | Complete demonstration app with Testcontainers verification suite. |

---

## Verification & Tests

Run the complete test suite (requires Docker):

```bash
./gradlew check
```

The integration test suite (`FykeScenariosTest`) spins up real PostgreSQL 16 and RabbitMQ containers and verifies:
1. **Transaction Rollback**: If domain transaction aborts, no event is published.
2. **Atomic Delivery**: Committed events are dispatched with sub-second latency.
3. **Idempotency**: Duplicate event inserts are discarded.
4. **Broker Outage Drain**: Backlog is queued in DB and drained cleanly once the broker recovers.
5. **Poison-Pill Capture & Replay**: Failing consumers write to `fyke_dlq` and can be replayed in-JVM.

---

## Documentation

- [AGENTS.md](AGENTS.md) — Implementation brief & quality constraints
- [docs/requirements-p1.md](docs/requirements-p1.md) — Acceptance criteria & specifications
- [docs/architecture.md](docs/architecture.md) — Component architecture & schema
- [docs/decisions.md](docs/decisions.md) — Architecture Decision Records (ADRs D-001 through D-016)
- [docs/validation.md](docs/validation.md) — Problem space validation & engineer feedback
- [CHANGELOG.md](CHANGELOG.md) — Version history

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
