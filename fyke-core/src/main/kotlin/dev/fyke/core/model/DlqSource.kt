package dev.fyke.core.model

/**
 * Origin source of an entry in the dead-letter queue table.
 */
enum class DlqSource {
	/** Failed during outbox publish attempts (producer side). */
	OUTBOX,

	/** Failed during message consumption after exhausting consumer retries (consumer side). */
	CONSUMER
}
