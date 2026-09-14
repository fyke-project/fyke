package dev.fyke.core.model

import java.time.Instant
import java.util.UUID

/**
 * Persistent representation of an outbox record stored in the database.
 */
data class OutboxRecord(
	val id: UUID,
	val seq: Long = 0,
	val partitionKey: String = "default",
	val type: String,
	val destination: String,
	val target: String? = null,
	val businessKey: String,
	val idempotencyKey: String,
	val status: OutboxStatus = OutboxStatus.NEW,
	val contentType: String = "application/json",
	val payload: ByteArray,
	val payloadHash: String,
	val headers: Map<String, String>? = null,
	val correlationId: String? = null,
	val traceId: String? = null,
	val size: Int = payload.size,
	val createdAt: Instant = Instant.now(),
	val updatedAt: Instant = Instant.now(),
	val publishedAt: Instant? = null,
	val attempts: Int = 0,
	val nextAttemptAt: Instant? = null,
	val leaseExpiresAt: Instant? = null
) {
	override fun equals(other: Any?): Boolean {
		if (this === other) return true
		if (javaClass != other?.javaClass) return false
		other as OutboxRecord
		return id == other.id
	}

	override fun hashCode(): Int = id.hashCode()
}
