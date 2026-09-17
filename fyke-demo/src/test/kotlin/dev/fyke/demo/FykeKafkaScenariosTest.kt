package dev.fyke.demo

import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.starter.Fyke
import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

@SpringBootTest
@Testcontainers
@ActiveProfiles("kafka")
class FykeKafkaScenariosTest {

	companion object {
		@Container
		@ServiceConnection
		val postgres = PostgreSQLContainer("postgres:16-alpine")

		@Container
		val kafka = KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"))

		@JvmStatic
		@DynamicPropertySource
		fun dynamicProperties(registry: DynamicPropertyRegistry) {
			registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers)
		}
	}

	@Autowired
	private lateinit var orderService: OrderService

	@Autowired
	private lateinit var orderConsumer: OrderConsumer

	@Autowired
	private lateinit var orderInboxConsumer: OrderInboxConsumer

	@Autowired
	private lateinit var outboxStore: OutboxStore

	@Autowired
	private lateinit var inboxStore: InboxStore

	@BeforeEach
	fun setUp() {
		orderConsumer.clear()
		orderInboxConsumer.clear()
	}

	@Test
	fun `Kafka Scenario 1 - End-to-end transactional publish and consumer receipt via Kafka`() {
		val orderId = "kafka-e2e-${UUID.randomUUID()}"
		orderService.createOrder(orderId, "Bob", 49.99)

		// Both plain consumer and transactional inbox consumer must receive the order
		await.atMost(Duration.ofSeconds(15)).untilAsserted {
			assertThat(orderConsumer.receivedOrders).contains(orderId)
			assertThat(orderInboxConsumer.receivedOrders).contains(orderId)
		}

		val summaries = Fyke.searchByBusinessKey(orderId)
		assertThat(summaries).isNotEmpty
		assertThat(summaries.any { it.source == "OUTBOX" && it.status == "PUBLISHED" }).isTrue()
		assertThat(summaries.any { it.source == "INBOX" && it.status == "COMPLETED" }).isTrue()
	}

	@Test
	fun `Kafka Scenario 2 - Inbox transient failure retries and eventual success`() {
		val orderId = "kafka-retry-${UUID.randomUUID()}"

		// Set to fail twice before succeeding
		orderService.createOrder(
			orderId = orderId,
			customer = "Charlie",
			amount = 99.0,
			failAttempts = 2,
		)

		// Initially the order fails transiently
		await.atMost(Duration.ofSeconds(15)).untilAsserted {
			val summaries = Fyke.searchByBusinessKey(orderId).filter { it.source == "INBOX" }
			assertThat(summaries).isNotEmpty
			assertThat(summaries.first().attempts).isGreaterThanOrEqualTo(1)
		}

		// Eventually it retries and succeeds
		await.atMost(Duration.ofSeconds(20)).untilAsserted {
			assertThat(orderInboxConsumer.receivedOrders).contains(orderId)
			val summaries = Fyke.searchByBusinessKey(orderId).filter { it.source == "INBOX" }
			assertThat(summaries.first().status).isEqualTo("COMPLETED")
		}
	}

	@Test
	fun `Kafka Scenario 3 - Inbox fatal poison pill fast-paths to DLQ`() {
		val fatalOrderId = "kafka-fatal-${UUID.randomUUID()}"

		orderService.createOrder(
			orderId = fatalOrderId,
			customer = "BadPayload",
			amount = 0.0,
			fatal = true,
		)

		await.atMost(Duration.ofSeconds(15)).untilAsserted {
			val summaries = Fyke.searchByBusinessKey(fatalOrderId)
			assertThat(summaries.any { it.source == "DLQ" && it.reason?.contains("fatal validation failure") == true }).isTrue()
			val inboxSummary = summaries.filter { it.source == "INBOX" }
			assertThat(inboxSummary).isNotEmpty
			assertThat(inboxSummary.first().status).isEqualTo("DEAD")
		}
	}

	@Test
	fun `Kafka Scenario 4 - Outbox failure simulation and replay with Kafka`() {
		val orderId = "kafka-outbox-sim-${UUID.randomUUID()}"

		// Send with outboxFatal = true so binder rejects it to DLQ
		orderService.createOrder(
			orderId = orderId,
			customer = "David",
			amount = 120.0,
			outboxFatal = true,
		)

		// Record should end up in DLQ
		var dlqId: UUID? = null
		await.atMost(Duration.ofSeconds(15)).untilAsserted {
			val summaries = Fyke.searchByBusinessKey(orderId).filter { it.source == "DLQ" }
			assertThat(summaries).isNotEmpty
			dlqId = summaries.first().id
		}

		assertThat(dlqId).isNotNull

		// Now trigger in-JVM replay
		val replayed = Fyke.replay(dlqId!!)
		assertThat(replayed).isTrue()

		// The replayed message must successfully land in Kafka and be received by inbox consumer
		await.atMost(Duration.ofSeconds(15)).untilAsserted {
			assertThat(orderInboxConsumer.receivedOrders).contains(orderId)
		}
	}
}
