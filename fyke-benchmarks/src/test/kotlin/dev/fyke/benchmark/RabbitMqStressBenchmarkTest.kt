package dev.fyke.benchmark

import dev.fyke.benchmark.harness.FifoSequenceVerifier
import dev.fyke.benchmark.harness.LatencyTracker
import dev.fyke.benchmark.harness.LoadGenerator
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.Binding
import org.springframework.amqp.core.BindingBuilder
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.Queue
import org.springframework.amqp.core.TopicExchange
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component
import org.springframework.test.context.TestPropertySource
import org.springframework.transaction.PlatformTransactionManager
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import java.util.UUID

@Tag("benchmark")
@SpringBootTest
@Testcontainers
@TestPropertySource(properties = [
	"fyke.binder=rabbitmq",
	"spring.rabbitmq.publisher-confirm-type=correlated",
	"spring.rabbitmq.publisher-returns=true"
])
class RabbitMqStressBenchmarkTest {

	companion object {
		private val log = LoggerFactory.getLogger(RabbitMqStressBenchmarkTest::class.java)

		const val EXCHANGE_NAME = "bench.rabbit.exchange"
		const val ROUTING_KEY = "bench.event"
		const val QUEUE_NAME = "bench.rabbit.queue"

		@Container
		@ServiceConnection
		val postgres: PostgreSQLContainer<*> = PostgreSQLContainer("postgres:16-alpine")
			.withCommand("postgres", "-c", "max_connections=250", "-c", "shared_buffers=256MB")

		@Container
		@ServiceConnection
		val rabbitmq: RabbitMQContainer = RabbitMQContainer("rabbitmq:3.13-management-alpine")
	}

	@TestConfiguration
	class RabbitTestConfig {
		@Bean
		fun benchQueue(): Queue = Queue(QUEUE_NAME, true)

		@Bean
		fun benchExchange(): TopicExchange = TopicExchange(EXCHANGE_NAME)

		@Bean
		fun benchBinding(benchQueue: Queue, benchExchange: TopicExchange): Binding =
			BindingBuilder.bind(benchQueue).to(benchExchange).with("bench.#")

		@Bean
		fun benchmarkRabbitConsumer(): BenchmarkRabbitConsumer = BenchmarkRabbitConsumer()
	}

	@Component
	class BenchmarkRabbitConsumer {
		var latencyTracker: LatencyTracker? = null
		var fifoVerifier: FifoSequenceVerifier? = null
		var loadGenerator: LoadGenerator? = null

		@RabbitListener(queues = [QUEUE_NAME])
		fun onMessage(message: Message) {
			val ackNanos = System.nanoTime()
			val eventIdStr = message.messageProperties.headers["x-fyke-event-id"]?.toString()
			if (eventIdStr != null) {
				try {
					val eventId = UUID.fromString(eventIdStr)
					val commitNanos = loadGenerator?.commitTimestamps?.remove(eventId)
					if (commitNanos != null) {
						latencyTracker?.recordNanos(ackNanos - commitNanos)
					}
				} catch (_: Exception) {}
			}

			val partitionKey = message.messageProperties.headers["x-fyke-partition-key"]?.toString() ?: "default"
			val seqStr = message.messageProperties.headers["x-fyke-seq"]?.toString()
				?: message.messageProperties.headers["x-benchmark-seq"]?.toString()
			val seq = seqStr?.toLongOrNull()
			if (seq != null) {
				fifoVerifier?.record(partitionKey, seq)
			}
		}
	}

	@Autowired
	private lateinit var transactionManager: PlatformTransactionManager

	@Autowired
	private lateinit var consumer: BenchmarkRabbitConsumer

	private lateinit var latencyTracker: LatencyTracker
	private lateinit var fifoVerifier: FifoSequenceVerifier
	private lateinit var loadGenerator: LoadGenerator

	@BeforeEach
	fun setUp() {
		latencyTracker = LatencyTracker()
		fifoVerifier = FifoSequenceVerifier()
		loadGenerator = LoadGenerator(transactionManager, latencyTracker, fifoVerifier)

		consumer.latencyTracker = latencyTracker
		consumer.fifoVerifier = fifoVerifier
		consumer.loadGenerator = loadGenerator
	}

	@Test
	fun `RabbitMQ Binder - 50 concurrent committers end-to-end stress and latency benchmark`() {
		val concurrency = 50
		val totalEvents = 2_000
		val partitionCount = 20
		val partitions = (1..partitionCount).map { "rabbit-tenant-$it" }

		log.info("Starting RabbitMQ benchmark: 50 concurrent committers submitting {} events...", totalEvents)

		val benchmarkStart = System.currentTimeMillis()
		val loadResult = loadGenerator.executeConcurrentLoad(
			concurrency = concurrency,
			totalEvents = totalEvents,
			partitionKeys = partitions,
			destination = EXCHANGE_NAME,
			routingKey = ROUTING_KEY
		)

		assertThat(loadResult.failedCommits).isEqualTo(0)
		assertThat(loadResult.successfulCommits).isEqualTo(totalEvents)

		log.info(
			"RabbitMQ: All {} events committed in {} ms ({} msg/s)",
			totalEvents,
			loadResult.commitDurationMs,
			"%.2f".format(totalEvents.toDouble() / (loadResult.commitDurationMs.toDouble() / 1000.0))
		)

		// Wait until all messages are received through RabbitMQ queue
		await.atMost(Duration.ofSeconds(45)).untilAsserted {
			assertThat(fifoVerifier.totalEvents()).isGreaterThanOrEqualTo(totalEvents)
		}
		val totalDurationMs = System.currentTimeMillis() - benchmarkStart

		val fifoResult = fifoVerifier.verifyStrictFifo()
		assertThat(fifoResult.isStrictFifo)
			.`as`("RabbitMQ delivery must maintain strict per-partition FIFO ordering. Errors: ${fifoResult.errorDetails}")
			.isTrue()

		val report = latencyTracker.generateReport(
			"RabbitMQ Binder: End-to-End Commit -> Publisher Confirm -> Consumer",
			totalDurationMs
		)
		println(report)
	}
}
