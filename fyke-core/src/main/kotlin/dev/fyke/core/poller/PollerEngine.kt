package dev.fyke.core.poller

import dev.fyke.core.binder.BrokerBinder
import dev.fyke.core.binder.PublishResult
import dev.fyke.core.model.OutboxRecord
import dev.fyke.core.model.OutboxStatus
import dev.fyke.core.partition.PartitionLocker
import dev.fyke.core.store.OutboxStore
import dev.fyke.core.telemetry.FykeTelemetry
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import javax.sql.DataSource
import kotlin.math.pow
import kotlin.random.Random

/**
 * Main coordinator managing batch claiming, partition mutual-exclusion, broker dispatch, and retry/DLQ lifecycles.
 */
class PollerEngine(
	private val outboxStore: OutboxStore,
	private val brokerBinder: BrokerBinder,
	private val partitionLocker: PartitionLocker,
	private val notificationSources: List<NotificationSource>,
	private val telemetry: FykeTelemetry,
	private val dataSource: DataSource,
	private val batchSize: Int = 50,
	private val leaseDuration: Duration = Duration.ofSeconds(30),
	private val maxAttempts: Int = 5,
	private val initialBackoffMs: Long = 1000L,
	private val backoffMultiplier: Double = 1.5
) {
	private val log = LoggerFactory.getLogger(javaClass)
	private val running = AtomicBoolean(false)
	private val polling = AtomicBoolean(false)
	private var executor: ExecutorService? = null

	fun start() {
		if (!running.compareAndSet(false, true)) return

		val exec = Executors.newSingleThreadExecutor { r ->
			Thread(r, "fyke-poller-engine").apply { isDaemon = true }
		}
		executor = exec

		notificationSources.forEach { source ->
			source.start {
				triggerPoll()
			}
		}

		// Trigger initial catch-up sweep
		triggerPoll()
		log.info("Fyke: Poller engine started (batchSize={}, leaseDuration={}, maxAttempts={})", batchSize, leaseDuration, maxAttempts)
	}

	fun stop() {
		if (running.compareAndSet(true, false)) {
			notificationSources.forEach { it.stop() }
			executor?.shutdownNow()
			executor = null
			log.info("Fyke: Poller engine stopped")
		}
	}

	fun triggerPoll() {
		if (!running.get()) return
		executor?.execute {
			if (polling.compareAndSet(false, true)) {
				try {
					var processed: Int
					do {
						processed = pollOnce()
					} while (processed > 0 && running.get())
				} catch (e: Exception) {
					log.error("Fyke: Unexpected error during outbox poll loop: {}", e.message, e)
				} finally {
					polling.set(false)
				}
			}
		}
	}

	fun pollOnce(): Int {
		val partitions = outboxStore.findPendingPartitions()
		if (partitions.isEmpty()) {
			telemetry.updateBacklogDepth(0)
			return 0
		}

		var totalProcessed = 0

		for (partition in partitions) {
			if (!running.get()) break

			var conn: Connection? = null
			try {
				conn = dataSource.connection
				conn.autoCommit = false

				val locked = partitionLocker.tryLock(partition, conn)
				if (!locked) {
					continue
				}

				try {
					val batch = outboxStore.claimBatch(batchSize, leaseDuration, partition)
					if (batch.isEmpty()) {
						continue
					}

					for (record in batch) {
						if (!running.get()) break
						processRecord(record)
						totalProcessed++
					}
				} finally {
					partitionLocker.unlock(partition, conn)
					conn.commit()
				}
			} catch (e: Exception) {
				log.warn("Fyke: Error processing partition '{}': {}", partition, e.message)
				try { conn?.rollback() } catch (_: Exception) {}
			} finally {
				try { conn?.close() } catch (_: Exception) {}
			}
		}

		val pending = outboxStore.countPending()
		telemetry.updateBacklogDepth(pending)
		return totalProcessed
	}

	private fun processRecord(record: OutboxRecord) {
		val startTime = System.currentTimeMillis()
		val result = try {
			brokerBinder.publish(record)
		} catch (e: Exception) {
			PublishResult.TransientFailure(e)
		}
		val durationMs = System.currentTimeMillis() - startTime
		when (result) {
			is PublishResult.Success -> {
				outboxStore.markPublished(record.id, Instant.now())
				telemetry.recordPublished(durationMs)
				log.debug("Fyke: Successfully published record {} (type={}, businessKey={}) in {} ms",
					record.id, record.type, record.businessKey, durationMs)
			}
			is PublishResult.TransientFailure -> {
				telemetry.recordPublishFailure()
				val nextAttempt = record.attempts + 1
				if (nextAttempt >= maxAttempts) {
					val reason = result.cause.message ?: "Max retry attempts ($maxAttempts) exhausted"
					log.error("Fyke: Record {} exhausted retries; marking DEAD: {}", record.id, reason)
					outboxStore.markDead(record.id, reason)
					telemetry.recordDlqMessage()
				} else {
					val backoff = calculateBackoff(nextAttempt)
					val nextAttemptAt = Instant.now().plusMillis(backoff)
					log.warn("Fyke: Publish failed for record {} (attempt {}/{}); retrying in {} ms: {}",
						record.id, nextAttempt, maxAttempts, backoff, result.cause.message)
					outboxStore.markRetry(record.id, nextAttempt, nextAttemptAt)
				}
			}
			is PublishResult.DeadLetter -> {
				telemetry.recordPublishFailure()
				telemetry.recordDlqMessage()
				log.error("Fyke: Record {} rejected by binder; moving to DLQ: {}", record.id, result.reason)
				outboxStore.markDead(record.id, result.reason)
			}
		}
	}

	fun replay(id: UUID): Boolean {
		// First check DLQ
		val dlq = outboxStore.findDlqById(id)
		if (dlq != null) {
			val record = OutboxRecord(
				id = dlq.id,
				partitionKey = dlq.partitionKey,
				type = dlq.type,
				destination = dlq.destination,
				target = dlq.target,
				businessKey = dlq.businessKey,
				idempotencyKey = UUID.randomUUID().toString(),
				status = OutboxStatus.DISPATCHING,
				contentType = dlq.contentType,
				payload = dlq.payload,
				payloadHash = "",
				headers = dlq.headers
			)
			val result = brokerBinder.publish(record)
			if (result is PublishResult.Success) {
				outboxStore.markDlqReplayed(id, Instant.now())
				log.info("Fyke: Replayed DLQ record {} to {}", id, dlq.destination)
				return true
			}
			return false
		}

		// Check Outbox
		val outbox = outboxStore.findOutboxById(id)
		if (outbox != null) {
			val result = brokerBinder.publish(outbox)
			if (result is PublishResult.Success) {
				outboxStore.markPublished(id, Instant.now())
				log.info("Fyke: Replayed outbox record {} to {}", id, outbox.destination)
				return true
			}
			return false
		}

		log.warn("Fyke: Cannot replay record {}: not found in DLQ or Outbox", id)
		return false
	}

	private fun calculateBackoff(attempt: Int): Long {
		val multiplier = backoffMultiplier.pow(attempt - 1)
		val jitter = Random.nextDouble(0.8, 1.2)
		return (initialBackoffMs * multiplier * jitter).toLong()
	}
}
