package dev.fyke.core.model

/**
 * Lifecycle status of an event in the outbox table.
 */
enum class OutboxStatus {
	/** Captured in the database transaction, awaiting dispatch. */
	NEW,

	/** Claimed by a poller instance and currently being published. */
	DISPATCHING,

	/** Successfully delivered to and confirmed by the message broker. */
	PUBLISHED,

	/** Exhausted maximum retry attempts or suffered unrecoverable failure; moved to DLQ. */
	DEAD
}
