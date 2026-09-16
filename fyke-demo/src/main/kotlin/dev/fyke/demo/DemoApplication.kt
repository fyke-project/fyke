package dev.fyke.demo

import dev.fyke.core.inbox.FykeListener
import dev.fyke.core.model.OrderingMode
import dev.fyke.starter.Fyke
import java.util.concurrent.CopyOnWriteArrayList
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.Binding
import org.springframework.amqp.core.BindingBuilder
import org.springframework.amqp.core.Queue
import org.springframework.amqp.core.TopicExchange
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
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
	}

	@Bean
	fun eventsExchange(): TopicExchange = TopicExchange(EXCHANGE_NAME)

	@Bean
	fun ordersQueue(): Queue = Queue(QUEUE_NAME, true)

	@Bean
	fun ordersBinding(ordersQueue: Queue, eventsExchange: TopicExchange): Binding {
		return BindingBuilder.bind(ordersQueue).to(eventsExchange).with(ROUTING_KEY)
	}

	@Bean
	fun ordersInboxQueue(): Queue = Queue(INBOX_QUEUE_NAME, true)

	@Bean
	fun ordersInboxBinding(ordersInboxQueue: Queue, eventsExchange: TopicExchange): Binding {
		return BindingBuilder.bind(ordersInboxQueue).to(eventsExchange).with(ROUTING_KEY)
	}

	@Bean
	fun jsonMessageConverter(objectMapper: com.fasterxml.jackson.databind.ObjectMapper): org.springframework.amqp.support.converter.MessageConverter {
		return org.springframework.amqp.support.converter.Jackson2JsonMessageConverter(objectMapper)
	}
}

data class OrderCreatedPayload(
	val orderId: String,
	val customer: String,
	val amount: Double,
)

@Service
class OrderService {

	@Transactional
	fun createOrder(orderId: String, customer: String, amount: Double) {
		val payload = OrderCreatedPayload(orderId, customer, amount)

		Fyke.send(
			type = "OrderCreated",
			destination = DemoApplication.EXCHANGE_NAME,
			target = DemoApplication.ROUTING_KEY,
			businessKey = orderId,
			payload = payload,
		)
	}
}

@Component
class OrderConsumer {

	val receivedOrders = CopyOnWriteArrayList<String>()
	var failOnPoison = true

	@RabbitListener(queues = [DemoApplication.QUEUE_NAME])
	fun handleOrder(payload: OrderCreatedPayload) {
		if (failOnPoison && payload.orderId.contains("poison", ignoreCase = true)) {
			throw RuntimeException("Simulated consumer poison pill for order: ${payload.orderId}")
		}
		receivedOrders.add(payload.orderId)
	}
}

@Component
class OrderInboxConsumer {

	val receivedOrders = CopyOnWriteArrayList<String>()
	var failForOrderId: String? = "42"
	var fatalError = false

	private val log = LoggerFactory.getLogger(javaClass)

	@FykeListener(
		destination = DemoApplication.INBOX_QUEUE_NAME,
		ordering = OrderingMode.STRICT_FIFO,
	)
	fun handleOrder(payload: OrderCreatedPayload) {
		if (fatalError) {
			throw IllegalArgumentException("Deterministic fatal validation failure for order: ${payload.orderId}")
		}
		if (failForOrderId == payload.orderId) {
			throw RuntimeException("Simulated transient failure for order: ${payload.orderId}")
		}
		receivedOrders.add(payload.orderId)

		log.info("Received order: {}", payload)
	}
}

fun main(args: Array<String>) {
	runApplication<DemoApplication>(*args)
}
