package dev.fyke.core.model

/**
 * Lifecycle status of an event in the transactional inbox.
 */
enum class InboxStatus {
	/** Captured from the message broker, awaiting local processing. */
	NEW,

	/** Claimed by an inbox poller worker and currently executing. */
	PROCESSING,

	/** Successfully processed by the consumer handler. */
	COMPLETED,

	/** Exhausted retries or suffered fatal poison-pill failure; moved to DLQ. */
	DEAD
}
