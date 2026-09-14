package dev.fyke.starter.annotation

/**
 * Marks a domain event class for automatic capture by Fyke when published via Spring's `ApplicationEventPublisher`.
 *
 * Example:
 * ```kotlin
 * @FykeEvent(
 *     destination = "orders.exchange",
 *     target = "order.created",
 *     type = "OrderCreatedEvent",
 *     businessKeyProperty = "orderId"
 * )
 * data class OrderCreatedEvent(val orderId: String, val amount: BigDecimal)
 * ```
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
annotation class FykeEvent(
	/**
	 * Broker destination where this event should be published (e.g. RabbitMQ exchange name or Kafka topic).
	 */
	val destination: String,

	/**
	 * Destination routing target (e.g. RabbitMQ routing key or Kafka partition key).
	 * If blank, defaults to [type] or the event class simple name.
	 */
	val target: String = "",

	/**
	 * Logical type identifier for the domain event.
	 * If blank, defaults to the event class simple name.
	 */
	val type: String = "",

	/**
	 * Name of the property/field on the event object that provides the business key (e.g. "orderId", "id").
	 * Defaults to "id".
	 */
	val businessKeyProperty: String = "id"
)
