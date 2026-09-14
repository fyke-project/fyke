package dev.fyke.core.model

import java.time.Instant
import java.util.UUID

/**
 * Persistent representation of a dead-letter entry stored in `fyke_dlq`.
 */
data class DlqRecord(
	val id: UUID = UUID.randomUUID(),
	val source: DlqSource,
	val outboxId: UUID? = null,
	val partitionKey: String = "default",
	val type: String,
	val destination: String,
	val target: String? = null,
	val businessKey: String,
	val contentType: String = "application/json",
	val payload: ByteArray,
	val headers: Map<String, String>? = null,
	val reason: String,
	val consumer: String? = null,
	val receivedAt: Instant = Instant.now(),
	val replayedAt: Instant? = null
) {
	override fun equals(other: Any?): Boolean {
		if (this === other) return true
		if (javaClass != other?.javaClass) return false
		other as DlqRecord
		return id == other.id
	}

	override fun hashCode(): Int = id.hashCode()
}
