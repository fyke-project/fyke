package dev.fyke.core.binder

import dev.fyke.core.model.OutboxRecord

/**
 * Pluggable SPI connecting Fyke's outbox engine to a concrete message broker (RabbitMQ, Kafka, etc.).
 *
 * Core outbox logic never depends directly on broker libraries.
 */
interface BrokerBinder {
	/** Identifies this binder implementation (e.g. "rabbitmq", "kafka"). */
	fun name(): String

	/**
	 * Publishes an outbox record to the broker.
	 *
	 * Implementations must wait for publisher confirmation / ACK before returning Success.
	 */
	fun publish(record: OutboxRecord): PublishResult

	/** Checks whether the broker connection is currently healthy. */
	fun isHealthy(): Boolean = true
}

/**
 * Result of a publish attempt returned by the BrokerBinder.
 */
sealed interface PublishResult {
	/** Confirmed receipt by the broker. */
	data object Success : PublishResult

	/** Temporary connection or timeout failure that should be retried. */
	data class TransientFailure(val cause: Throwable) : PublishResult

	/** Unrecoverable or rejected message that should be routed to DLQ immediately without retrying. */
	data class DeadLetter(val reason: String, val cause: Throwable? = null) : PublishResult
}
