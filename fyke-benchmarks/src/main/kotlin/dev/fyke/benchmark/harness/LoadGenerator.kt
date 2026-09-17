package dev.fyke.benchmark.harness

import dev.fyke.core.model.OutboxRecord
import dev.fyke.starter.Fyke
import org.slf4j.LoggerFactory
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionSynchronization
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.transaction.support.TransactionTemplate
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Payload carried by benchmark events.
 */
data class BenchmarkPayload(
	val id: String,
	val sequence: Int,
	val partitionKey: String,
	val timestampNanos: Long = System.nanoTime(),
	val padding: String = "x".repeat(128)
)

/**
 * Concurrent load generator simulating multiple simultaneous transactional committers.
 */
class LoadGenerator(
	private val transactionManager: PlatformTransactionManager,
	val latencyTracker: LatencyTracker,
	val fifoVerifier: FifoSequenceVerifier
) {
	private val log = LoggerFactory.getLogger(javaClass)
	val commitTimestamps = ConcurrentHashMap<UUID, Long>()
	private val partitionSeqCounters = ConcurrentHashMap<String, AtomicInteger>()

	/**
	 * Callback to be invoked when an outbox record is published.
	 */
	fun onRecordPublished(record: OutboxRecord) {
		val ackNanos = System.nanoTime()
		val commitNanos = commitTimestamps.remove(record.id)
		if (commitNanos != null) {
			val latencyNanos = ackNanos - commitNanos
			latencyTracker.recordNanos(latencyNanos)
		}

		fifoVerifier.record(record.partitionKey, record.seq)
	}

	/**
	 * Executes concurrent transactional commits across a thread pool.
	 *
	 * @param concurrency Number of concurrent committer threads (e.g. 50).
	 * @param totalEvents Total number of events to commit across all threads.
	 * @param partitionKeys List of partition keys to distribute load across.
	 * @param destination Target broker destination (exchange/topic).
	 * @param routingKey Target routing key.
	 */
	fun executeConcurrentLoad(
		concurrency: Int,
		totalEvents: Int,
		partitionKeys: List<String>,
		destination: String,
		routingKey: String = "bench-key"
	): LoadRunResult {
		val pool = Executors.newFixedThreadPool(concurrency) { r ->
			Thread(r, "fyke-benchmark-committer").apply { isDaemon = true }
		}

		val transactionTemplate = TransactionTemplate(transactionManager)
		val startLatch = CountDownLatch(1)
		val doneLatch = CountDownLatch(totalEvents)
		val successfulCommits = AtomicInteger(0)
		val failedCommits = AtomicInteger(0)

		val startTime = System.currentTimeMillis()

		for (i in 0 until totalEvents) {
			val partitionKey = partitionKeys[i % partitionKeys.size]
			val seq = partitionSeqCounters.computeIfAbsent(partitionKey) { AtomicInteger(0) }.incrementAndGet()
			val eventId = "bench-$partitionKey-$seq-${UUID.randomUUID()}"

			pool.submit {
				try {
					startLatch.await()
					transactionTemplate.execute {
						val payload = BenchmarkPayload(
							id = eventId,
							sequence = seq,
							partitionKey = partitionKey
						)
						val record = Fyke.send(
							type = "BenchmarkEvent",
							destination = destination,
							target = routingKey,
							businessKey = eventId,
							payload = payload,
							partitionKey = partitionKey,
							headers = mapOf(
								"x-fyke-partition-key" to partitionKey,
								"x-benchmark-seq" to seq.toString()
							)
						)

						TransactionSynchronizationManager.registerSynchronization(object : TransactionSynchronization {
							override fun afterCommit() {
								val commitNano = System.nanoTime()
								commitTimestamps[record.id] = commitNano
							}
						})
					}
					successfulCommits.incrementAndGet()
				} catch (e: Exception) {
					log.error("Commit failed for event $eventId: {}", e.message)
					failedCommits.incrementAndGet()
				} finally {
					doneLatch.countDown()
				}
			}
		}

		// Unleash all committer threads simultaneously
		startLatch.countDown()

		val completed = doneLatch.await(120, TimeUnit.SECONDS)
		val commitDurationMs = System.currentTimeMillis() - startTime

		pool.shutdown()
		pool.awaitTermination(5, TimeUnit.SECONDS)

		return LoadRunResult(
			totalRequested = totalEvents,
			successfulCommits = successfulCommits.get(),
			failedCommits = failedCommits.get(),
			commitDurationMs = commitDurationMs,
			completedInTime = completed
		)
	}

	fun reset() {
		commitTimestamps.clear()
		partitionSeqCounters.clear()
		latencyTracker.reset()
		fifoVerifier.reset()
	}
}

data class LoadRunResult(
	val totalRequested: Int,
	val successfulCommits: Int,
	val failedCommits: Int,
	val commitDurationMs: Long,
	val completedInTime: Boolean
)
