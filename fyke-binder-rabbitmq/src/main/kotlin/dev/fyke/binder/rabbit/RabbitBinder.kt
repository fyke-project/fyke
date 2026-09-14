package dev.fyke.binder.rabbit

import dev.fyke.core.binder.BrokerBinder
import dev.fyke.core.binder.PublishResult
import dev.fyke.core.model.OutboxRecord
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageDeliveryMode
import org.springframework.amqp.core.MessageProperties
import org.springframework.amqp.rabbit.connection.CorrelationData
import org.springframework.amqp.rabbit.core.RabbitTemplate

/**
 * RabbitMQ implementation of BrokerBinder (R3) using Spring AMQP and publisher confirms.
 */
class RabbitBinder(
	private val rabbitTemplate: RabbitTemplate,
	private val confirmTimeoutMs: Long = 5000L,
) : BrokerBinder {

	private val log = LoggerFactory.getLogger(javaClass)

	override fun name(): String = "rabbitmq"

	override fun publish(record: OutboxRecord): PublishResult {
		val properties = MessageProperties().apply {
			contentType = record.contentType
			messageId = record.id.toString()
			correlationId = record.correlationId
			deliveryMode = MessageDeliveryMode.PERSISTENT

			setHeader("x-fyke-idempotency-key", record.idempotencyKey)
			setHeader("x-fyke-event-id", record.id.toString())
			setHeader("x-fyke-business-key", record.businessKey)
			setHeader("x-fyke-type", record.type)
			setHeader("x-fyke-partition-key", record.partitionKey)

			record.headers?.forEach { (key, value) ->
				setHeader(key, value)
			}
		}

		val message = Message(record.payload, properties)
		val exchange = record.destination
		val routingKey = record.target ?: record.type

		val correlationData = CorrelationData(record.id.toString())

		return try {
			rabbitTemplate.send(exchange, routingKey, message, correlationData)
			val confirm = correlationData.future.get(confirmTimeoutMs, TimeUnit.MILLISECONDS)
			if (confirm != null && confirm.ack) {
				PublishResult.Success
			} else {
				val reason = confirm?.reason() ?: "NACK received without reason"
				log.warn("Fyke: RabbitMQ publisher NACK for record {}: {}", record.id, reason)
				PublishResult.TransientFailure(RuntimeException("RabbitMQ publish NACK: $reason"))
			}
		} catch (e: TimeoutException) {
			log.warn(
				"Fyke: RabbitMQ publisher confirm timed out after {} ms for record {}",
				confirmTimeoutMs,
				record.id,
			)
			PublishResult.TransientFailure(e)
		} catch (e: Exception) {
			log.warn("Fyke: RabbitMQ publish error for record {}: {}", record.id, e.message)
			PublishResult.TransientFailure(e)
		}
	}

	override fun isHealthy(): Boolean {
		return try {
			rabbitTemplate.connectionFactory.createConnection().use { conn ->
				conn.isOpen
			}
		} catch (_: Exception) {
			false
		}
	}
}
