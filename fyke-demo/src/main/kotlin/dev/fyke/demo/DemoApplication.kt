package dev.fyke.demo

import dev.fyke.core.binder.BrokerBinder
import dev.fyke.core.binder.PublishResult
import dev.fyke.core.inbox.FykeListener
import dev.fyke.core.model.OrderingMode
import dev.fyke.core.model.OutboxRecord
import dev.fyke.starter.Fyke
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.Binding
import org.springframework.amqp.core.BindingBuilder
import org.springframework.amqp.core.Queue
import org.springframework.amqp.core.TopicExchange
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.beans.factory.annotation.Value
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@SpringBootApplication
class DemoApplication {

	companion object {
		const val EXCHANGE_NAME = "events.exchange"
		const val DESTINATION = EXCHANGE_NAME
		const val QUEUE_NAME = "orders.queue"
		const val INBOX_QUEUE_NAME = "orders.inbox.queue"
		const val ROUTING_KEY = "orders.created"
		const val TARGET = ROUTING_KEY
		const val KAFKA_TOPIC = "orders.events"
	}
}

@Configuration
@Profile("!kafka")
class RabbitConfig {

	@Bean
	fun eventsExchange(): TopicExchange = TopicExchange(DemoApplication.EXCHANGE_NAME)

	@Bean
	fun ordersQueue(): Queue = Queue(DemoApplication.QUEUE_NAME, true)

	@Bean
	fun ordersBinding(ordersQueue: Queue, eventsExchange: TopicExchange): Binding {
		return BindingBuilder.bind(ordersQueue).to(eventsExchange).with(DemoApplication.ROUTING_KEY)
	}

	@Bean
	fun ordersInboxQueue(): Queue = Queue(DemoApplication.INBOX_QUEUE_NAME, true)

	@Bean
	fun ordersInboxBinding(ordersInboxQueue: Queue, eventsExchange: TopicExchange): Binding {
		return BindingBuilder.bind(ordersInboxQueue).to(eventsExchange).with(DemoApplication.ROUTING_KEY)
	}

	@Bean
	fun jsonMessageConverter(): org.springframework.amqp.support.converter.MessageConverter {
		return org.springframework.amqp.support.converter.JacksonJsonMessageConverter()
	}
}

@Configuration
@Profile("kafka")
class KafkaConfig {

	@Bean
	fun ordersTopic(): org.apache.kafka.clients.admin.NewTopic {
		return org.apache.kafka.clients.admin.NewTopic(DemoApplication.KAFKA_TOPIC, 1, 1.toShort())
	}
}

data class OrderCreatedPayload(
	val orderId: String,
	val customer: String,
	val amount: Double,
	val failAttempts: Int = 0,
	val fatal: Boolean = false,
)

@Service
class OrderService(
	@Value("\${demo.destination:events.exchange}") private val defaultDestination: String,
	@Value("\${demo.target:orders.created}") private val defaultTarget: String,
) {

	@Transactional
	fun createOrder(
		orderId: String,
		customer: String,
		amount: Double,
		failAttempts: Int = 0,
		fatal: Boolean = false,
		outboxFailAttempts: Int = 0,
		outboxFatal: Boolean = false,
		headers: Map<String, String>? = null,
		partitionKey: String? = null,
		destination: String? = null,
		target: String? = null,
	): OutboxRecord {
		val payload = OrderCreatedPayload(
			orderId = orderId,
			customer = customer,
			amount = amount,
			failAttempts = failAttempts,
			fatal = fatal,
		)

		val combinedHeaders = mutableMapOf<String, String>()
		headers?.let { combinedHeaders.putAll(it) }
		if (failAttempts > 0) {
			combinedHeaders["x-fail-attempts"] = failAttempts.toString()
		}
		if (fatal) {
			combinedHeaders["x-fatal"] = "true"
		}
		if (outboxFailAttempts > 0) {
			combinedHeaders["x-fyke-simulate-outbox-fail"] = outboxFailAttempts.toString()
		}
		if (outboxFatal) {
			combinedHeaders["x-fyke-simulate-outbox-dead"] = "true"
		}

		return Fyke.send(
			type = "OrderCreated",
			destination = destination ?: defaultDestination,
			target = target ?: defaultTarget,
			businessKey = orderId,
			payload = payload,
			partitionKey = partitionKey ?: "default",
			headers = combinedHeaders.ifEmpty { null },
		)
	}
}

@Component
class OrderConsumer {

	val receivedOrders = CopyOnWriteArrayList<String>()
	var failOnPoison = true

	fun track(orderId: String) {
		if (failOnPoison && orderId.contains("poison", ignoreCase = true)) {
			throw RuntimeException("Simulated consumer poison pill for order: $orderId")
		}
		receivedOrders.add(orderId)
	}

	fun clear() {
		receivedOrders.clear()
		failOnPoison = true
	}
}

@Component
@Profile("!kafka")
class RabbitOrderListener(
	private val orderConsumer: OrderConsumer,
) {

	@RabbitListener(queues = [DemoApplication.QUEUE_NAME])
	fun handleOrder(payload: OrderCreatedPayload) {
		orderConsumer.track(payload.orderId)
	}
}

@Component
@Profile("kafka")
class KafkaOrderListener(
	private val orderConsumer: OrderConsumer,
	private val objectMapper: com.fasterxml.jackson.databind.ObjectMapper,
) {

	@KafkaListener(topics = ["\${demo.destination:orders.events}"], groupId = "fyke-demo-plain-group")
	fun handleOrder(record: org.apache.kafka.clients.consumer.ConsumerRecord<*, *>) {
		val rawValue = record.value()
		val payload = try {
			when (rawValue) {
				is OrderCreatedPayload -> rawValue
				is ByteArray -> objectMapper.readValue(rawValue, OrderCreatedPayload::class.java)
				is String -> objectMapper.readValue(rawValue, OrderCreatedPayload::class.java)
				else -> objectMapper.readValue(rawValue.toString(), OrderCreatedPayload::class.java)
			}
		} catch (_: Exception) {
			null
		}
		if (payload != null) {
			orderConsumer.track(payload.orderId)
		}
	}
}

@Component
class OrderInboxConsumer {

	val receivedOrders = CopyOnWriteArrayList<String>()
	val failureCountPerOrder = ConcurrentHashMap<String, AtomicInteger>()
	var failForOrderId: String? = "42"
	var fatalError = false

	private val log = LoggerFactory.getLogger(javaClass)

	@FykeListener(
		destination = "\${demo.inbox-destination:orders.inbox.queue}",
		ordering = OrderingMode.STRICT_FIFO,
	)
	fun handleOrder(payload: OrderCreatedPayload) {
		val orderId = payload.orderId
		log.info("Fyke demo: Inbox consumer processing order '{}' (failAttempts={}, fatal={})", orderId, payload.failAttempts, payload.fatal)

		if (payload.fatal || fatalError) {
			log.error("Fyke demo: Triggering simulated fatal error for order '{}'", orderId)
			throw IllegalArgumentException("Deterministic fatal validation failure for order: $orderId")
		}

		val attemptsFailed = failureCountPerOrder.computeIfAbsent(orderId) { AtomicInteger(0) }

		if (payload.failAttempts > 0 && attemptsFailed.get() < payload.failAttempts) {
			val current = attemptsFailed.incrementAndGet()
			log.warn("Fyke demo: Simulated transient failure (attempt {}/{}) for order '{}'", current, payload.failAttempts, orderId)
			throw RuntimeException("Simulated transient failure (attempt $current of ${payload.failAttempts}) for order: $orderId")
		}

		if (failForOrderId == orderId && attemptsFailed.get() < 1) {
			val current = attemptsFailed.incrementAndGet()
			log.warn("Fyke demo: Simulated transient failure for order '{}'", orderId)
			throw RuntimeException("Simulated transient failure for order: $orderId")
		}

		receivedOrders.add(orderId)
		log.info("Fyke demo: Successfully processed order '{}' in inbox", orderId)
	}

	fun clear() {
		receivedOrders.clear()
		failureCountPerOrder.clear()
		failForOrderId = null
		fatalError = false
	}
}

class DelegatingTestableBrokerBinder(
	private val delegate: BrokerBinder,
) : BrokerBinder {

	private val log = LoggerFactory.getLogger(javaClass)
	private val failureCounters = ConcurrentHashMap<String, AtomicInteger>()
	private val deadSimulated = ConcurrentHashMap<String, Boolean>()

	override fun name(): String = delegate.name()

	override fun publish(record: OutboxRecord): PublishResult {
		val key = record.businessKey.ifBlank { record.id.toString() }
		val simulateDead = record.headers?.get("x-fyke-simulate-outbox-dead") == "true"
		if (simulateDead && deadSimulated.putIfAbsent(key, true) == null) {
			log.warn("Fyke demo: Simulating dead-letter rejection by binder for businessKey '{}' (id={})", key, record.id)
			return PublishResult.DeadLetter("Simulated outbox dead letter for record ${record.id}")
		}

		val simulateFailAttempts = record.headers?.get("x-fyke-simulate-outbox-fail")?.toIntOrNull() ?: 0
		if (simulateFailAttempts > 0) {
			val counter = failureCounters.computeIfAbsent(key) { AtomicInteger(0) }
			val attempt = counter.incrementAndGet()
			if (attempt <= simulateFailAttempts) {
				log.warn(
					"Fyke demo: Simulating transient publish failure (attempt {}/{}) for businessKey '{}'",
					attempt,
					simulateFailAttempts,
					key
				)
				return PublishResult.TransientFailure(
					RuntimeException("Simulated outbox publish failure (attempt $attempt of $simulateFailAttempts)")
				)
			}
		}

		return delegate.publish(record)
	}

	override fun isHealthy(): Boolean = delegate.isHealthy()
}

@Component
class TestableBrokerBinderBeanPostProcessor : BeanPostProcessor {

	override fun postProcessAfterInitialization(bean: Any, beanName: String): Any {
		if (bean is BrokerBinder && bean !is DelegatingTestableBrokerBinder) {
			return DelegatingTestableBrokerBinder(bean)
		}
		return bean
	}
}

fun main(args: Array<String>) {
	runApplication<DemoApplication>(*args)
}
