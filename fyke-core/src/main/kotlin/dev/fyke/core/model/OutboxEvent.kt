package dev.fyke.core.model

import java.util.UUID

/**
 * Domain event payload and routing instructions submitted to Fyke for transactional delivery.
 *
 * @property type Event classification name (e.g. "OrderCreated").
 * @property destination Target broker destination (e.g. RabbitMQ exchange, Kafka topic, or Webhook URL).
 * @property target Optional sub-target or routing key (e.g. RabbitMQ routing key, Kafka message key).
 * @property businessKey Business identifier used for search and ordering (e.g. order_id, vin_number).
 * @property payload Raw domain object, string, or byte array to serialize and deliver.
 * @property idempotencyKey Unique key for deduplication. Defaults to a random UUID.
 * @property partitionKey Optional partition key. If null, resolved via the configured PartitionResolver.
 * @property headers Optional broker headers or metadata.
 * @property correlationId Optional tracing correlation ID.
 */
data class OutboxEvent(
	val type: String,
	val destination: String,
	val target: String? = null,
	val businessKey: String,
	val payload: Any,
	val idempotencyKey: String = UUID.randomUUID().toString(),
	val partitionKey: String? = null,
	val headers: Map<String, String>? = null,
	val correlationId: String? = null
)
