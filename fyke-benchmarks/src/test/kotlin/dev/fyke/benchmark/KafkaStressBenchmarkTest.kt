package dev.fyke.benchmark

import dev.fyke.benchmark.harness.FifoSequenceVerifier
import dev.fyke.benchmark.harness.LatencyTracker
import dev.fyke.benchmark.harness.LoadGenerator
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.TopicBuilder
import org.springframework.stereotype.Component
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.context.TestPropertySource
import org.springframework.transaction.PlatformTransactionManager
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.UUID

@Tag("benchmark")
@SpringBootTest
@Testcontainers
@TestPropertySource(properties = [
	"fyke.binder=kafka",
	"spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
	"spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.ByteArraySerializer",
	"spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
	"spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer",
	"spring.kafka.consumer.auto-offset-reset=earliest",
	"spring.kafka.consumer.group-id=benchmark-kafka-group"
])
class KafkaStressBenchmarkTest {

	companion object {
		private val log = LoggerFactory.getLogger(KafkaStressBenchmarkTest::class.java)

		const val TOPIC_NAME = "bench.kafka.events"

		@Container
		@ServiceConnection
		val postgres: PostgreSQLContainer<*> = PostgreSQLContainer("postgres:16-alpine")
			.withCommand("postgres", "-c", "max_connections=250", "-c", "shared_buffers=256MB")

		@Container
		val kafka: KafkaContainer = KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"))

		@JvmStatic
		@DynamicPropertySource
		fun registerKafkaProperties(registry: DynamicPropertyRegistry) {
			registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers)
		}
	}

	@TestConfiguration
	class KafkaTestConfig {
		@Bean
		fun benchTopic(): NewTopic = TopicBuilder.name(TOPIC_NAME).partitions(10).replicas(1).build()

		@Bean
		fun benchmarkKafkaConsumer(): BenchmarkKafkaConsumer = BenchmarkKafkaConsumer()
	}

	@Component
	class BenchmarkKafkaConsumer {
		var latencyTracker: LatencyTracker? = null
		var fifoVerifier: FifoSequenceVerifier? = null
		var loadGenerator: LoadGenerator? = null

		@KafkaListener(topics = [TOPIC_NAME], groupId = "benchmark-kafka-group")
		fun onMessage(record: ConsumerRecord<String, ByteArray>) {
			val ackNanos = System.nanoTime()

			val eventIdHeader = record.headers().lastHeader("x-fyke-event-id")
			if (eventIdHeader != null) {
				try {
					val eventIdStr = String(eventIdHeader.value(), StandardCharsets.UTF_8)
					val eventId = UUID.fromString(eventIdStr)
					val commitNanos = loadGenerator?.commitTimestamps?.remove(eventId)
					if (commitNanos != null) {
						latencyTracker?.recordNanos(ackNanos - commitNanos)
					}
				} catch (_: Exception) {}
			}

			val partHeader = record.headers().lastHeader("x-fyke-partition-key")
			val partitionKey = if (partHeader != null) String(partHeader.value(), StandardCharsets.UTF_8) else "default"

			val seqHeader = record.headers().lastHeader("x-fyke-seq") ?: record.headers().lastHeader("x-benchmark-seq")
			if (seqHeader != null) {
				val seq = String(seqHeader.value(), StandardCharsets.UTF_8).toLongOrNull()
				if (seq != null) {
					fifoVerifier?.record(partitionKey, seq)
				}
			}
		}
	}

	@Autowired
	private lateinit var transactionManager: PlatformTransactionManager

	@Autowired
	private lateinit var consumer: BenchmarkKafkaConsumer

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
	fun `Kafka Binder - 50 concurrent committers end-to-end stress and latency benchmark`() {
		val concurrency = 50
		val totalEvents = 2_000
		val partitionCount = 20
		val partitions = (1..partitionCount).map { "kafka-tenant-$it" }

		log.info("Starting Kafka benchmark: 50 concurrent committers submitting {} events...", totalEvents)

		val benchmarkStart = System.currentTimeMillis()
		val loadResult = loadGenerator.executeConcurrentLoad(
			concurrency = concurrency,
			totalEvents = totalEvents,
			partitionKeys = partitions,
			destination = TOPIC_NAME,
			routingKey = "bench-key"
		)

		assertThat(loadResult.failedCommits).isEqualTo(0)
		assertThat(loadResult.successfulCommits).isEqualTo(totalEvents)

		log.info(
			"Kafka: All {} events committed in {} ms ({} msg/s)",
			totalEvents,
			loadResult.commitDurationMs,
			"%.2f".format(totalEvents.toDouble() / (loadResult.commitDurationMs.toDouble() / 1000.0))
		)

		// Wait until all messages are received through Kafka consumer
		await.atMost(Duration.ofSeconds(45)).untilAsserted {
			assertThat(fifoVerifier.totalEvents()).isGreaterThanOrEqualTo(totalEvents)
		}
		val totalDurationMs = System.currentTimeMillis() - benchmarkStart

		val fifoResult = fifoVerifier.verifyStrictFifo()
		assertThat(fifoResult.isStrictFifo)
			.`as`("Kafka delivery must maintain strict per-partition FIFO ordering. Errors: ${fifoResult.errorDetails}")
			.isTrue()

		val report = latencyTracker.generateReport(
			"Kafka Binder: End-to-End Commit -> Producer ACK -> Consumer",
			totalDurationMs
		)
		println(report)
	}
}
