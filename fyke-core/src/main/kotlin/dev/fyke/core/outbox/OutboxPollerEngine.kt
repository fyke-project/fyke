package dev.fyke.core.outbox

import dev.fyke.core.binder.BrokerBinder
import dev.fyke.core.binder.PublishResult
import dev.fyke.core.model.OutboxRecord
import dev.fyke.core.model.OutboxStatus
import dev.fyke.core.partition.PartitionLocker
import dev.fyke.core.poller.AbstractPollerEngine
import dev.fyke.core.poller.BackoffPolicy
import dev.fyke.core.poller.NotificationSource
import dev.fyke.core.telemetry.FykeTelemetry
import java.time.Duration
import java.time.Instant
import java.util.UUID
import javax.sql.DataSource

/**
 * Main coordinator managing outbox batch claiming, partition mutual-exclusion, broker dispatch, and retry/DLQ lifecycles.
 */
class OutboxPollerEngine(
	private val outboxStore: OutboxStore,
	private val brokerBinder: BrokerBinder,
	partitionLocker: PartitionLocker,
	notificationSources: List<NotificationSource>,
	private val telemetry: FykeTelemetry,
	dataSource: DataSource,
	batchSize: Int = 50,
	leaseDuration: Duration = Duration.ofSeconds(30),
	private val maxAttempts: Int = 5,
	initialBackoffMs: Long = 1000L,
	backoffMultiplier: Double = 1.5,
	concurrency: Int = 1
) : AbstractPollerEngine<OutboxRecord>(
	partitionLocker = partitionLocker,
	notificationSources = notificationSources,
	dataSource = dataSource,
	batchSize = batchSize,
	leaseDuration = leaseDuration,
	concurrency = concurrency,
	threadPrefix = "fyke-outbox"
) {

	private val backoffPolicy = BackoffPolicy(
		initialBackoffMs = initialBackoffMs,
		backoffMultiplier = backoffMultiplier,
		withJitter = true
	)

	override fun findPendingPartitions(): List<String> {
		return outboxStore.findPendingPartitions()
	}

	override fun claimBatch(partition: String): List<OutboxRecord> {
		val batch = outboxStore.claimBatch(batchSize, leaseDuration, partition)
		for (record in batch) {
			telemetry.notifyOutboxStatusChanged(record, OutboxStatus.NEW, OutboxStatus.DISPATCHING)
		}
		return batch
	}

	override fun onNoPendingPartitions() {
		log.trace("Fyke: Outbox poller found no pending partitions; backlog depth set to 0")
		telemetry.updateBacklogDepth(0)
	}

	override fun onBatchCompleted() {
		val pending = outboxStore.countPending()
		log.trace("Fyke: Outbox poller batch sweep completed; backlog depth={}", pending)
		telemetry.updateBacklogDepth(pending)
	}

	override fun processRecord(record: OutboxRecord) {
		log.trace(
			"Fyke: Dispatching outbox record id={} (attempt={}, partitionKey={}, destination={})",
			record.id,
			record.attempts,
			record.partitionKey,
			record.destination
		)
		val startTime = System.currentTimeMillis()
		val result = try {
			brokerBinder.publish(record)
		} catch (e: Exception) {
			PublishResult.TransientFailure(e)
		}
		val durationMs = System.currentTimeMillis() - startTime
		log.trace("Fyke: Binder result for outbox record id={}: {} (took {} ms)", record.id, result, durationMs)
		when (result) {
			is PublishResult.Success -> {
				outboxStore.markPublished(record.id, Instant.now())
				telemetry.recordPublished(durationMs)
				telemetry.notifyOutboxStatusChanged(record.copy(status = OutboxStatus.PUBLISHED), OutboxStatus.DISPATCHING, OutboxStatus.PUBLISHED)
				log.debug(
					"Fyke: Successfully published record {} (type={}, businessKey={}) in {} ms",
					record.id, record.type, record.businessKey, durationMs
				)
			}
			is PublishResult.TransientFailure -> {
				telemetry.recordPublishFailure()
				val nextAttempt = record.attempts + 1
				if (nextAttempt >= maxAttempts) {
					val reason = result.cause.message ?: "Max retry attempts ($maxAttempts) exhausted"
					log.error("Fyke: Record {} exhausted retries; marking DEAD: {}", record.id, reason)
					outboxStore.markDead(record.id, reason)
					telemetry.recordDlqMessage()
					telemetry.notifyOutboxStatusChanged(record.copy(status = OutboxStatus.DEAD, attempts = nextAttempt), OutboxStatus.DISPATCHING, OutboxStatus.DEAD, reason)
				} else {
					val backoff = backoffPolicy.calculate(nextAttempt)
					val nextAttemptAt = Instant.now().plusMillis(backoff)
					log.warn(
						"Fyke: Publish failed for record {} (attempt {}/{}); retrying in {} ms: {}",
						record.id, nextAttempt, maxAttempts, backoff, result.cause.message
					)
					outboxStore.markRetry(record.id, nextAttempt, nextAttemptAt)
					telemetry.notifyOutboxStatusChanged(record.copy(status = OutboxStatus.NEW, attempts = nextAttempt, nextAttemptAt = nextAttemptAt), OutboxStatus.DISPATCHING, OutboxStatus.NEW, result.cause.message)
				}
			}
			is PublishResult.DeadLetter -> {
				telemetry.recordPublishFailure()
				telemetry.recordDlqMessage()
				log.error("Fyke: Record {} rejected by binder; moving to DLQ: {}", record.id, result.reason)
				outboxStore.markDead(record.id, result.reason)
				telemetry.notifyOutboxStatusChanged(record.copy(status = OutboxStatus.DEAD), OutboxStatus.DISPATCHING, OutboxStatus.DEAD, result.reason)
			}
		}
	}

	fun replay(id: UUID): Boolean {
		log.debug("Fyke: Replay requested for record id={}", id)
		// First check DLQ
		val dlq = outboxStore.findDlqById(id)
		if (dlq != null) {
			log.trace("Fyke: Found record id={} in DLQ, publishing to {}", id, dlq.destination)
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
				telemetry.notifyOutboxStatusChanged(record.copy(status = OutboxStatus.PUBLISHED), OutboxStatus.DEAD, OutboxStatus.PUBLISHED)
				log.info("Fyke: Replayed DLQ record {} to {}", id, dlq.destination)
				return true
			}
			log.warn("Fyke: Failed to replay DLQ record id={}: {}", id, result)
			return false
		}

		// Check Outbox
		val outbox = outboxStore.findOutboxById(id)
		if (outbox != null) {
			log.trace("Fyke: Found record id={} in Outbox, publishing to {}", id, outbox.destination)
			val result = brokerBinder.publish(outbox)
			if (result is PublishResult.Success) {
				outboxStore.markPublished(id, Instant.now())
				telemetry.notifyOutboxStatusChanged(outbox.copy(status = OutboxStatus.PUBLISHED), outbox.status, OutboxStatus.PUBLISHED)
				log.info("Fyke: Replayed outbox record {} to {}", id, outbox.destination)
				return true
			}
			log.warn("Fyke: Failed to replay outbox record id={}: {}", id, result)
			return false
		}

		log.warn("Fyke: Cannot replay record {}: not found in DLQ or Outbox", id)
		return false
	}
}
