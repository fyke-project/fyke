package dev.fyke.core.model

import java.time.Instant
import java.util.UUID

/**
 * Persistent representation of an incoming message captured into the transactional inbox table (`fyke_inbox`).
 */
data class InboxRecord(
	val id: UUID,
	val seq: Long = 0,
	val partitionKey: String = "default",
	val type: String,
	val destination: String,
	val target: String? = null,
	val businessKey: String,
	val messageId: String? = null,
	val status: InboxStatus = InboxStatus.NEW,
	val contentType: String = "application/json",
	val payload: ByteArray,
	val headers: Map<String, String>? = null,
	val consumer: String? = null,
	val ordering: OrderingMode = OrderingMode.STRICT_FIFO,
	val createdAt: Instant = Instant.now(),
	val updatedAt: Instant = Instant.now(),
	val completedAt: Instant? = null,
	val attempts: Int = 0,
	val nextAttemptAt: Instant? = null,
	val leaseExpiresAt: Instant? = null
) {
	override fun equals(other: Any?): Boolean {
		if (this === other) return true
		if (javaClass != other?.javaClass) return false
		other as InboxRecord
		return id == other.id
	}

	override fun hashCode(): Int = id.hashCode()
}
