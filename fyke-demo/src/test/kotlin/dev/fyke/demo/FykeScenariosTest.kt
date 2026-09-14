package dev.fyke.demo

import com.fasterxml.jackson.databind.ObjectMapper
import dev.fyke.core.model.OutboxEvent
import dev.fyke.core.model.OutboxRecord
import dev.fyke.core.model.OutboxStatus
import dev.fyke.core.store.DuplicateIdempotencyKeyException
import dev.fyke.core.store.OutboxStore
import dev.fyke.starter.Fyke
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import java.time.Instant
import java.util.UUID

@SpringBootTest
@Testcontainers
class FykeScenariosTest {

	companion object {
		@Container
		@ServiceConnection
		val postgres = PostgreSQLContainer("postgres:16-alpine")

		@Container
		@ServiceConnection
		val rabbitmq = RabbitMQContainer("rabbitmq:3.13-management-alpine")
	}

	@Autowired
	private lateinit var orderService: OrderService

	@Autowired
	private lateinit var orderConsumer: OrderConsumer

	@Autowired
	private lateinit var outboxStore: OutboxStore

	private val objectMapper: ObjectMapper = com.fasterxml.jackson.module.kotlin.jacksonObjectMapper().apply {
		registerModule(com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
	}

	@Autowired
	private lateinit var transactionManager: PlatformTransactionManager

	@BeforeEach
	fun setUp() {
		orderConsumer.receivedOrders.clear()
		orderConsumer.failOnPoison = true
	}

	@Test
	fun `Scenario 1 - Pod death mid-transaction recovery (R1, R2, R9)`() {
		val orderId = "order-crash-${UUID.randomUUID()}"
		val payloadBytes = objectMapper.writeValueAsBytes(OrderCreatedPayload(orderId, "Alice", 199.99))

		// Simulate crash: outbox row written in DB before pod died
		val record = OutboxRecord(
			id = UUID.randomUUID(),
			partitionKey = "default",
			type = "OrderCreated",
			destination = DemoApplication.EXCHANGE_NAME,
			target = DemoApplication.ROUTING_KEY,
			businessKey = orderId,
			idempotencyKey = "idem-$orderId",
			status = OutboxStatus.NEW,
			contentType = "application/json",
			payload = payloadBytes,
			payloadHash = "hash-$orderId"
		)
		outboxStore.save(record)

		// The running poller must claim the orphaned row and publish it
		await.atMost(Duration.ofSeconds(10)).untilAsserted {
			assertThat(orderConsumer.receivedOrders).contains(orderId)
			val updated = outboxStore.findOutboxById(record.id)
			assertThat(updated?.status).isEqualTo(OutboxStatus.PUBLISHED)
		}
	}

	@Test
	fun `Scenario 2 - End-to-end transactional publish and consumer receipt (R1, R3)`() {
		val orderId = "order-e2e-${UUID.randomUUID()}"
		orderService.createOrder(orderId, "Bob", 49.99)

		await.atMost(Duration.ofSeconds(10)).untilAsserted {
			assertThat(orderConsumer.receivedOrders).contains(orderId)
		}

		val summaries = Fyke.searchByBusinessKey(orderId)
		assertThat(summaries).isNotEmpty
		assertThat(summaries.first().status).isEqualTo("PUBLISHED")
	}

	@Test
	fun `Scenario 3 - Poison pill capture, business key search, and in-JVM replay (R4, R5, R9)`() {
		val poisonOrderId = "order-poison-${UUID.randomUUID()}"

		// Send order that fails in consumer
		orderService.createOrder(poisonOrderId, "Mallory", 999.99)

		// Wait until consumer failure is captured in fyke_dlq
		await.atMost(Duration.ofSeconds(10)).untilAsserted {
			val summaries = Fyke.searchByBusinessKey(poisonOrderId)
			assertThat(summaries.any { it.source == "DLQ" || it.reason != null }).isTrue()
		}

		val dlqSummaries = Fyke.searchByBusinessKey(poisonOrderId).filter { it.source == "DLQ" }
		assertThat(dlqSummaries).hasSize(1)
		val dlqEntry = dlqSummaries.first()
		assertThat(dlqEntry.reason).contains("Simulated consumer poison pill")

		// Fix consumer condition
		orderConsumer.failOnPoison = false

		// Trigger in-JVM replay
		val replayed = Fyke.replay(dlqEntry.id)
		assertThat(replayed).isTrue()

		// Verify successful receipt after replay
		await.atMost(Duration.ofSeconds(10)).untilAsserted {
			assertThat(orderConsumer.receivedOrders).contains(poisonOrderId)
		}
	}

	@Test
	fun `Scenario 4 - Idempotency deduplication rejects duplicate idempotency key (R1)`() {
		val idemKey = "fixed-idempotency-${UUID.randomUUID()}"
		val event1 = OutboxEvent(
			type = "OrderCreated",
			destination = DemoApplication.EXCHANGE_NAME,
			target = DemoApplication.ROUTING_KEY,
			businessKey = "order-idem-1",
			payload = OrderCreatedPayload("order-idem-1", "Eve", 10.0),
			idempotencyKey = idemKey
		)
		val event2 = OutboxEvent(
			type = "OrderCreated",
			destination = DemoApplication.EXCHANGE_NAME,
			target = DemoApplication.ROUTING_KEY,
			businessKey = "order-idem-2",
			payload = OrderCreatedPayload("order-idem-2", "Eve", 10.0),
			idempotencyKey = idemKey
		)

		Fyke.send(event1)

		assertThatThrownBy { Fyke.send(event2) }
			.isInstanceOf(DuplicateIdempotencyKeyException::class.java)
	}

	@Test
	fun `Scenario 5 - Transaction rollback leaves no outbox row (R1)`() {
		val orderId = "order-rollback-${UUID.randomUUID()}"
		val tt = TransactionTemplate(transactionManager)

		try {
			tt.execute {
				Fyke.send(
					type = "OrderCreated",
					destination = DemoApplication.EXCHANGE_NAME,
					target = DemoApplication.ROUTING_KEY,
					businessKey = orderId,
					payload = OrderCreatedPayload(orderId, "Ghost", 0.0)
				)
				throw RuntimeException("Intentional domain failure causing rollback")
			}
		} catch (_: RuntimeException) {}

		// Assert nothing was saved in DB
		val summaries = Fyke.searchByBusinessKey(orderId)
		assertThat(summaries).isEmpty()
		assertThat(orderConsumer.receivedOrders).doesNotContain(orderId)
	}
}
