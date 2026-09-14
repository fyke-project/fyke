package dev.fyke.core.model

import java.time.Instant
import java.util.UUID

/**
 * Summary view returned when querying events across outbox and DLQ tables by business key.
 */
data class FykeRecordSummary(
	val id: UUID,
	val source: String, // "OUTBOX" or "DLQ"
	val type: String,
	val businessKey: String,
	val status: String,
	val destination: String,
	val target: String?,
	val timestamp: Instant,
	val attempts: Int = 0,
	val reason: String? = null
)
