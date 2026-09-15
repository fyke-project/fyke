package dev.fyke.core.inbox

import dev.fyke.core.model.OrderingMode

/**
 * Marks a method as an event handler backed by the transactional inbox (`fyke_inbox`).
 *
 * Messages delivered from the message broker to [destination] are first transactionally
 * stored into `fyke_inbox` and immediately acknowledged to the broker. The inbox poller
 * then invokes this method with partition locking, ordered retries, and dead-letter protection.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
annotation class FykeListener(
	/**
	 * The incoming destination to consume from:
	 * - RabbitMQ: Queue name (e.g. "orders.queue")
	 * - Kafka: Topic name (e.g. "orders.topic")
	 */
	val destination: String,

	/**
	 * Optional consumer group identifier (e.g. Kafka consumer group).
	 */
	val consumerGroup: String = "",

	/**
	 * Ordering behavior for this consumer:
	 * - [OrderingMode.STRICT_FIFO]: Failing events pause subsequent events for the same partition.
	 * - [OrderingMode.LEAPFROG]: Subsequent events in the partition proceed even while an event is retrying.
	 */
	val ordering: OrderingMode = OrderingMode.STRICT_FIFO,

	/**
	 * Optional property name on the payload object to extract as the local partition key.
	 * If empty, the incoming message header ('x-fyke-partition-key' or 'x-fyke-business-key') is used.
	 */
	val partitionKeyProperty: String = "",

	/**
	 * Optional bean name of a custom [ConsumerPartitionResolver] in the Spring context.
	 */
	val partitionResolverBean: String = "",

	/**
	 * Number of concurrent worker threads.
	 */
	val concurrency: Int = 1
)
