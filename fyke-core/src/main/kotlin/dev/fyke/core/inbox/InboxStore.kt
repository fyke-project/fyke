package dev.fyke.core.inbox

import dev.fyke.core.model.FykeRecordSummary
import dev.fyke.core.model.InboxRecord
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * Storage interface for managing persistent inbox records in `fyke_inbox`.
 */
interface InboxStore {
	/**
	 * Persists an incoming message into the inbox.
	 * If messageId is provided and already exists, it is considered a duplicate and returns false.
	 *
	 * @return true if successfully saved, false if skipped due to duplicate messageId.
	 */
	fun save(record: InboxRecord): Boolean

	/**
	 * Finds an inbox record by its UUID.
	 */
	fun findById(id: UUID): InboxRecord?

	/**
	 * Finds all distinct partitions that have pending records ('NEW' or 'PROCESSING' with expired lease).
	 */
	fun findPendingPartitions(): List<String>

	/**
	 * Claims a batch of ready records for the given partitionKey, respecting its ordering mode:
	 * - STRICT_FIFO: Earlier uncompleted records in backoff block newer records.
	 * - LEAPFROG: Newer records can execute while earlier records are waiting.
	 */
	fun claimBatch(batchSize: Int, leaseDuration: Duration, partitionKey: String): List<InboxRecord>

	/**
	 * Marks an inbox record as successfully processed.
	 */
	fun markCompleted(id: UUID, completedAt: Instant)

	/**
	 * Schedules an inbox record for retry with updated attempt count and next attempt timestamp.
	 */
	fun markRetry(id: UUID, attempts: Int, nextAttemptAt: Instant)

	/**
	 * Marks an inbox record as DEAD.
	 */
	fun markDead(id: UUID)

	/**
	 * Resets nextAttemptAt to now() for an inbox record, immediately waking up its partition.
	 *
	 * @return true if record was found and updated, false otherwise.
	 */
	fun retryNow(id: UUID): Boolean

	/**
	 * Searches inbox records by business key, mapped to FykeRecordSummary with source = "INBOX".
	 */
	fun searchByBusinessKey(businessKey: String): List<FykeRecordSummary>

	/**
	 * Purges completed records older than the cutoff timestamp.
	 */
	fun purgeCompleted(cutoff: Instant, batchSize: Int): Int

	/**
	 * Counts the total number of pending inbox records.
	 */
	fun countPending(): Long
}
