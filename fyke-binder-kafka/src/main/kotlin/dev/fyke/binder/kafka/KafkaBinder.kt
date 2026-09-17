package dev.fyke.binder.kafka

import dev.fyke.core.binder.BrokerBinder
import dev.fyke.core.binder.PublishResult
import dev.fyke.core.model.OutboxRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * Apache Kafka implementation of BrokerBinder (R3) using Spring Kafka with publisher confirmations.
 */
class KafkaBinder(
	private val kafkaTemplate: KafkaTemplate<String, ByteArray>,
	private val confirmTimeoutMs: Long = 5000L
) : BrokerBinder {

	private val log = LoggerFactory.getLogger(javaClass)

	override fun name(): String = "kafka"

	override fun publish(record: OutboxRecord): PublishResult {
		val topic = record.destination
		val messageKey = record.target ?: record.businessKey

		val producerRecord = ProducerRecord<String, ByteArray>(topic, messageKey, record.payload)

		producerRecord.headers().apply {
			add(RecordHeader("x-fyke-idempotency-key", record.idempotencyKey.toByteArray(StandardCharsets.UTF_8)))
			add(RecordHeader("x-fyke-event-id", record.id.toString().toByteArray(StandardCharsets.UTF_8)))
			add(RecordHeader("x-fyke-business-key", record.businessKey.toByteArray(StandardCharsets.UTF_8)))
			add(RecordHeader("x-fyke-type", record.type.toByteArray(StandardCharsets.UTF_8)))
			add(RecordHeader("x-fyke-partition-key", record.partitionKey.toByteArray(StandardCharsets.UTF_8)))
			add(RecordHeader("x-fyke-seq", record.seq.toString().toByteArray(StandardCharsets.UTF_8)))
			add(RecordHeader("x-fyke-content-type", record.contentType.toByteArray(StandardCharsets.UTF_8)))

			record.correlationId?.let {
				add(RecordHeader("x-fyke-correlation-id", it.toByteArray(StandardCharsets.UTF_8)))
			}
			record.traceId?.let {
				add(RecordHeader("x-fyke-trace-id", it.toByteArray(StandardCharsets.UTF_8)))
			}

			record.headers?.forEach { (key, value) ->
				add(RecordHeader(key, value.toByteArray(StandardCharsets.UTF_8)))
			}
		}

		log.debug(
			"Fyke: Publishing record id={} (type={}, businessKey={}) to Kafka topic '{}' with key '{}'",
			record.id,
			record.type,
			record.businessKey,
			topic,
			messageKey
		)
		log.trace("Fyke: Kafka message headers for record id={}: {}", record.id, record.headers)

		return try {
			val sendFuture = kafkaTemplate.send(producerRecord)
			val sendResult = sendFuture.get(confirmTimeoutMs, TimeUnit.MILLISECONDS)
			val metadata = sendResult.recordMetadata

			log.trace(
				"Fyke: Received Kafka ACK for record id={} (topic={}, partition={}, offset={})",
				record.id,
				metadata.topic(),
				metadata.partition(),
				metadata.offset()
			)
			PublishResult.Success
		} catch (e: TimeoutException) {
			log.warn(
				"Fyke: Kafka publisher confirm timed out after {} ms for record {}",
				confirmTimeoutMs,
				record.id
			)
			PublishResult.TransientFailure(e)
		} catch (e: Exception) {
			val cause = e.cause ?: e
			log.warn("Fyke: Kafka publish error for record {}: {}", record.id, cause.message)
			PublishResult.TransientFailure(cause)
		}
	}

	override fun isHealthy(): Boolean {
		val healthy = try {
			kafkaTemplate.producerFactory.createProducer().use { true }
		} catch (_: Exception) {
			false
		}
		log.trace("Fyke: Kafka isHealthy check returned {}", healthy)
		return healthy
	}
}
