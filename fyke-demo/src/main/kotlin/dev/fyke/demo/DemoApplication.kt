package dev.fyke.demo

import dev.fyke.starter.Fyke
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
import java.util.concurrent.CopyOnWriteArrayList

@SpringBootApplication
class DemoApplication {

	companion object {
		const val EXCHANGE_NAME = "events.exchange"
		const val QUEUE_NAME = "orders.queue"
		const val ROUTING_KEY = "orders.created"
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
	fun jsonMessageConverter(objectMapper: com.fasterxml.jackson.databind.ObjectMapper): org.springframework.amqp.support.converter.MessageConverter {
		return org.springframework.amqp.support.converter.Jackson2JsonMessageConverter(objectMapper)
	}
}

data class OrderCreatedPayload(
	val orderId: String,
	val customer: String,
	val amount: Double
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
			payload = payload
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

fun main(args: Array<String>) {
	runApplication<DemoApplication>(*args)
}
