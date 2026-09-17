package dev.fyke.binder.kafka

import dev.fyke.core.binder.PublishResult
import dev.fyke.core.model.OutboxRecord
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeoutException

class KafkaBinderTest {

	@Suppress("UNCHECKED_CAST")
	private val kafkaTemplate = mockk<KafkaTemplate<String, ByteArray>>()
	private val binder = KafkaBinder(kafkaTemplate, confirmTimeoutMs = 1000L)

	private val sampleRecord = OutboxRecord(
		id = UUID.randomUUID(),
		type = "OrderCreated",
		destination = "orders.topic",
		target = "order-123",
		businessKey = "order-123",
		idempotencyKey = "idem-123",
		payload = "{}".toByteArray(StandardCharsets.UTF_8),
		payloadHash = "hash123"
	)

	@Test
	fun `should publish message and return Success when acknowledged`() {
		val recordSlot = slot<ProducerRecord<String, ByteArray>>()
		val future = CompletableFuture<SendResult<String, ByteArray>>()

		val metadata = RecordMetadata(TopicPartition("orders.topic", 0), 0L, 0, 0L, 0, 0)
		val sendResult = SendResult(sampleRecord.toProducerRecord(), metadata)
		future.complete(sendResult)

		every { kafkaTemplate.send(capture(recordSlot)) } returns future

		val result = binder.publish(sampleRecord)

		assertThat(result).isEqualTo(PublishResult.Success)
		val captured = recordSlot.captured
		assertThat(captured.topic()).isEqualTo("orders.topic")
		assertThat(captured.key()).isEqualTo("order-123")

		val headers = captured.headers().associate { it.key() to String(it.value(), StandardCharsets.UTF_8) }
		assertThat(headers["x-fyke-idempotency-key"]).isEqualTo("idem-123")
		assertThat(headers["x-fyke-business-key"]).isEqualTo("order-123")
		assertThat(headers["x-fyke-type"]).isEqualTo("OrderCreated")
		assertThat(headers["x-fyke-partition-key"]).isEqualTo("default")
	}

	@Test
	fun `should propagate traceId header when present`() {
		val recordSlot = slot<ProducerRecord<String, ByteArray>>()
		val future = CompletableFuture<SendResult<String, ByteArray>>()
		val metadata = RecordMetadata(TopicPartition("orders.topic", 0), 0L, 0, 0L, 0, 0)
		future.complete(SendResult(sampleRecord.toProducerRecord(), metadata))

		every { kafkaTemplate.send(capture(recordSlot)) } returns future

		val recordWithTrace = sampleRecord.copy(traceId = "0af7651916cd43dd8448eb211c80319c")
		binder.publish(recordWithTrace)

		val headers = recordSlot.captured.headers().associate { it.key() to String(it.value(), StandardCharsets.UTF_8) }
		assertThat(headers["x-fyke-trace-id"]).isEqualTo("0af7651916cd43dd8448eb211c80319c")
	}

	@Test
	fun `should return TransientFailure when future times out`() {
		val future = CompletableFuture<SendResult<String, ByteArray>>()
		future.completeExceptionally(TimeoutException("Kafka timeout"))

		every { kafkaTemplate.send(any<ProducerRecord<String, ByteArray>>()) } returns future

		val result = binder.publish(sampleRecord)

		assertThat(result).isInstanceOf(PublishResult.TransientFailure::class.java)
	}

	@Test
	fun `should return TransientFailure when future completes with exception`() {
		val future = CompletableFuture<SendResult<String, ByteArray>>()
		future.completeExceptionally(RuntimeException("Broker not available"))

		every { kafkaTemplate.send(any<ProducerRecord<String, ByteArray>>()) } returns future

		val result = binder.publish(sampleRecord)

		assertThat(result).isInstanceOf(PublishResult.TransientFailure::class.java)
		val failure = result as PublishResult.TransientFailure
		assertThat(failure.cause.message).contains("Broker not available")
	}

	private fun OutboxRecord.toProducerRecord(): ProducerRecord<String, ByteArray> {
		return ProducerRecord(destination, target ?: businessKey, payload)
	}
}
