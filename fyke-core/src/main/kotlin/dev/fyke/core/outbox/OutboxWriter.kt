package dev.fyke.core.outbox

import dev.fyke.core.model.OutboxEvent
import dev.fyke.core.model.OutboxRecord

/**
 * Capture interface for writing events to the outbox (D-005).
 *
 * Implemented synchronously for JDBC in P1, with an architectural seam for reactive R2DBC in P1.x.
 */
interface OutboxWriter {
	/**
	 * Captures the domain event into the active transaction's outbox.
	 *
	 * @return The persisted OutboxRecord.
	 */
	fun write(event: OutboxEvent): OutboxRecord
}
