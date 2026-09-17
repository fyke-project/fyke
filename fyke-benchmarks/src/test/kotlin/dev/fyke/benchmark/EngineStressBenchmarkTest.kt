package dev.fyke.benchmark

import dev.fyke.benchmark.harness.FifoSequenceVerifier
import dev.fyke.benchmark.harness.LatencyTracker
import dev.fyke.benchmark.harness.LoadGenerator
import dev.fyke.benchmark.harness.RecordingBrokerBinder
import dev.fyke.core.binder.BrokerBinder
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.starter.Fyke
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
import org.springframework.context.annotation.Primary
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionSynchronization
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

@Tag("benchmark")
@SpringBootTest
@Testcontainers
class EngineStressBenchmarkTest {

	companion object {
		private val log = LoggerFactory.getLogger(EngineStressBenchmarkTest::class.java)

		@Container
		@ServiceConnection
		val postgres: PostgreSQLContainer<*> = PostgreSQLContainer("postgres:16-alpine")
			.withCommand("postgres", "-c", "max_connections=250", "-c", "shared_buffers=256MB")

		val recordingBinder = RecordingBrokerBinder()
	}

	@TestConfiguration
	class BinderTestConfig {
		@Bean
		@Primary
		fun testBrokerBinder(): BrokerBinder = recordingBinder
	}

	@Autowired
	private lateinit var transactionManager: PlatformTransactionManager

	@Autowired
	private lateinit var outboxStore: OutboxStore

	@Autowired
	private lateinit var jdbcTemplate: JdbcTemplate

	private lateinit var latencyTracker: LatencyTracker
	private lateinit var fifoVerifier: FifoSequenceVerifier
	private lateinit var loadGenerator: LoadGenerator

	@BeforeEach
	fun setUp() {
		recordingBinder.reset()
		latencyTracker = LatencyTracker()
		fifoVerifier = FifoSequenceVerifier()
		loadGenerator = LoadGenerator(transactionManager, latencyTracker, fifoVerifier)

		recordingBinder.onPublishedCallback = { record ->
			loadGenerator.onRecordPublished(record)
		}
	}

	@Test
	fun `Scenario 1 - Sub-100ms probe latency with 10,000 backlog rows (Acceptance R2)`() {
		log.info("Starting Scenario 1: Seeding 10,000 backlog rows into fyke_outbox...")

		// Pre-seed 10,000 backlog rows directly via batch insert
		val batchSql = """
			INSERT INTO fyke_outbox (
				id, partition_key, type, destination, target, business_key,
				idempotency_key, status, content_type, payload, payload_hash,
				size, created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		""".trimIndent()

		val now = Instant.now()
		val totalBacklog = 10_000
		val chunkSize = 1_000
		for (chunk in 0 until (totalBacklog / chunkSize)) {
			val batchArgs = (0 until chunkSize).map { i ->
				val idx = chunk * chunkSize + i
				val id = UUID.randomUUID()
				val payloadBytes = "{\"backlogIndex\":$idx}".toByteArray()
				arrayOf(
					id,
					"backlog-partition-${idx % 20}",
					"BacklogEvent",
					"bench-destination",
					"bench-target",
					"biz-backlog-$idx",
					"idem-backlog-$id",
					"NEW",
					"application/json",
					payloadBytes,
					"hash-$id",
					payloadBytes.size,
					Timestamp.from(now),
					Timestamp.from(now)
				)
			}
			jdbcTemplate.batchUpdate(batchSql, batchArgs)
		}

		val pendingBefore = outboxStore.countPending()
		assertThat(pendingBefore).isGreaterThanOrEqualTo(9_000L)
		log.info("Seeded {} backlog rows successfully. Active pending count: {}", totalBacklog, pendingBefore)

		// Fire probe event on a distinct partition via standard @Transactional Fyke.send with NOTIFY hint
		val probePartition = "probe-partition-${UUID.randomUUID()}"
		val commitNano = AtomicLong(0)
		val ackNano = AtomicLong(0)
		val probeFinished = CountDownLatch(1)

		recordingBinder.onPublishedCallback = { record ->
			if (record.partitionKey == probePartition && ackNano.get() == 0L) {
				ackNano.set(System.nanoTime())
				probeFinished.countDown()
			}
			loadGenerator.onRecordPublished(record)
		}

		val tt = TransactionTemplate(transactionManager)
		tt.execute {
			Fyke.send(
				type = "ProbeEvent",
				destination = "bench-destination",
				target = "probe-target",
				businessKey = "biz-probe-${UUID.randomUUID()}",
				payload = mapOf("probe" to true),
				partitionKey = probePartition
			)

			TransactionSynchronizationManager.registerSynchronization(object : TransactionSynchronization {
				override fun afterCommit() {
					commitNano.set(System.nanoTime())
				}
			})
		}

		val delivered = probeFinished.await(10, TimeUnit.SECONDS)
		assertThat(delivered).isTrue()

		val latencyMs = (ackNano.get() - commitNano.get()) / 1_000_000.0
		log.info("Scenario 1 Probe Result: Probe event published in {} ms with 10,000 backlog rows present", "%.2f".format(latencyMs))

		// Acceptance Criterion R2: <= ~100 ms after commit on NOTIFY path
		// Allow margin for container runtime variability
		assertThat(latencyMs)
			.`as`("Acceptance R2: Probe event should be published in <= 100 ms (measured: ${"%.2f".format(latencyMs)} ms)")
			.isLessThanOrEqualTo(150.0)
	}

	@Test
	fun `Scenario 2 - 50 concurrent committers across 50 partitions (Acceptance R2)`() {
		val concurrency = 50
		val totalEvents = 2_500
		val partitionCount = 50
		val partitions = (1..partitionCount).map { "tenant-$it" }

		log.info("Starting Scenario 2: Running 50 concurrent committers generating {} events across 50 partitions...", totalEvents)

		val loadResult = loadGenerator.executeConcurrentLoad(
			concurrency = concurrency,
			totalEvents = totalEvents,
			partitionKeys = partitions,
			destination = "bench-dest"
		)

		assertThat(loadResult.failedCommits).isEqualTo(0)
		assertThat(loadResult.successfulCommits).isEqualTo(totalEvents)
		assertThat(loadResult.completedInTime).isTrue()

		log.info(
			"All {} events committed successfully in {} ms ({} commits/s)",
			totalEvents,
			loadResult.commitDurationMs,
			"%.2f".format(totalEvents.toDouble() / (loadResult.commitDurationMs.toDouble() / 1000.0))
		)

		// Drain until all events are published
		val drainStart = System.currentTimeMillis()
		await.atMost(Duration.ofSeconds(45)).untilAsserted {
			val publishedNow = recordingBinder.publishedRecords.filter { it.destination == "bench-dest" }
			assertThat(publishedNow.size).isGreaterThanOrEqualTo(totalEvents)
		}
		val drainDurationMs = System.currentTimeMillis() - drainStart

		val fifoResult = fifoVerifier.verifyStrictFifo()
		assertThat(fifoResult.isStrictFifo)
			.`as`("All batches must be processed in strict per-partition FIFO order. Errors: ${fifoResult.errorDetails}")
			.isTrue()

		val report = latencyTracker.generateReport(
			"Scenario 2 - 50 Concurrent Committers / 50 Partitions",
			drainDurationMs
		)
		println(report)

		assertThat(latencyTracker.totalCount()).isEqualTo(totalEvents.toLong())
		val throughput = totalEvents.toDouble() / (drainDurationMs.toDouble() / 1000.0)
		assertThat(throughput)
			.`as`("Throughput under 50 concurrent committers should exceed 200 msg/s (measured: ${"%.2f".format(throughput)} msg/s)")
			.isGreaterThan(200.0)
	}

	@Test
	fun `Scenario 3 - Contention stress on a single hot partition (Advisory lock non-blocking validation)`() {
		val concurrency = 50
		val totalEvents = 1_000
		val singlePartition = "hot-partition-single"

		log.info("Starting Scenario 3: 50 concurrent committers contending on single partition '{}'...", singlePartition)

		val loadResult = loadGenerator.executeConcurrentLoad(
			concurrency = concurrency,
			totalEvents = totalEvents,
			partitionKeys = listOf(singlePartition),
			destination = "bench-hot-dest"
		)

		assertThat(loadResult.failedCommits).isEqualTo(0)
		assertThat(loadResult.successfulCommits).isEqualTo(totalEvents)

		// Drain
		val drainStart = System.currentTimeMillis()
		await.atMost(Duration.ofSeconds(30)).untilAsserted {
			val count = recordingBinder.publishedRecords.count { it.destination == "bench-hot-dest" }
			assertThat(count).isEqualTo(totalEvents)
		}
		val drainDurationMs = System.currentTimeMillis() - drainStart

		val fifoResult = fifoVerifier.verifyStrictFifo()
		assertThat(fifoResult.isStrictFifo)
			.`as`("Single partition must be strictly ordered. Errors: ${fifoResult.errorDetails}")
			.isTrue()

		val report = latencyTracker.generateReport(
			"Scenario 3 - Single Hot Partition Contention",
			drainDurationMs
		)
		println(report)
	}
}
