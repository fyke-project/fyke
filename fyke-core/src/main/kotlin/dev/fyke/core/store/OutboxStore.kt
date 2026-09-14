package dev.fyke.core.store

import dev.fyke.core.model.DlqRecord
import dev.fyke.core.model.FykeRecordSummary
import dev.fyke.core.model.OutboxRecord
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * Storage interface for managing persistent outbox and DLQ records.
 */
interface OutboxStore {
	fun save(record: OutboxRecord)
	fun findOutboxById(id: UUID): OutboxRecord?
	fun findPendingPartitions(): List<String>
	fun claimBatch(batchSize: Int, leaseDuration: Duration, partitionKey: String): List<OutboxRecord>
	fun markPublished(id: UUID, publishedAt: Instant)
	fun markRetry(id: UUID, attempts: Int, nextAttemptAt: Instant)
	fun markDead(id: UUID, reason: String)

	fun saveDlq(dlq: DlqRecord)
	fun findDlqById(id: UUID): DlqRecord?
	fun markDlqReplayed(id: UUID, replayedAt: Instant)

	fun searchByBusinessKey(businessKey: String): List<FykeRecordSummary>

	fun purgePublished(cutoff: Instant, batchSize: Int): Int
	fun purgeDlq(cutoff: Instant, batchSize: Int): Int
	fun countPending(): Long
}

class DuplicateIdempotencyKeyException(key: String) :
	RuntimeException("Outbox event with idempotency_key '$key' already exists")
