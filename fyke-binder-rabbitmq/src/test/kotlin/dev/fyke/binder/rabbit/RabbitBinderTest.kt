package dev.fyke.binder.rabbit

import dev.fyke.core.binder.PublishResult
import dev.fyke.core.model.OutboxRecord
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.amqp.core.Message
import org.springframework.amqp.rabbit.connection.CorrelationData
import org.springframework.amqp.rabbit.core.RabbitTemplate
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeoutException

class RabbitBinderTest {

	private val rabbitTemplate = mockk<RabbitTemplate>()
	private val binder = RabbitBinder(rabbitTemplate, confirmTimeoutMs = 1000L)

	private val sampleRecord = OutboxRecord(
		id = UUID.randomUUID(),
		type = "OrderCreated",
		destination = "orders.exchange",
		target = "orders.created",
		businessKey = "order-123",
		idempotencyKey = "idem-123",
		payload = "{}".toByteArray(Charsets.UTF_8),
		payloadHash = "hash123"
	)

	@Test
	fun `should publish message and return Success when confirmed`() {
		val messageSlot = slot<Message>()
		val correlationSlot = slot<CorrelationData>()

		every {
			rabbitTemplate.send(eq("orders.exchange"), eq("orders.created"), capture(messageSlot), capture(correlationSlot))
		} answers {
			val cd = correlationSlot.captured
			val confirm = CorrelationData.Confirm(true, null)
			cd.future.complete(confirm)
		}

		val result = binder.publish(sampleRecord)

		assertThat(result).isEqualTo(PublishResult.Success)
		val sentMessage = messageSlot.captured
		assertThat(sentMessage.messageProperties.headers["x-fyke-idempotency-key"]).isEqualTo("idem-123")
		assertThat(sentMessage.messageProperties.headers["x-fyke-business-key"]).isEqualTo("order-123")
		assertThat(sentMessage.messageProperties.headers["x-fyke-type"]).isEqualTo("OrderCreated")
	}

	@Test
	fun `should propagate traceId header when present`() {
		val messageSlot = slot<Message>()
		val correlationSlot = slot<CorrelationData>()

		every {
			rabbitTemplate.send(any<String>(), any<String>(), capture(messageSlot), capture(correlationSlot))
		} answers {
			correlationSlot.captured.future.complete(CorrelationData.Confirm(true, null))
		}

		val recordWithTrace = sampleRecord.copy(traceId = "0af7651916cd43dd8448eb211c80319c")
		binder.publish(recordWithTrace)

		val sentMessage = messageSlot.captured
		assertThat(sentMessage.messageProperties.headers["x-fyke-trace-id"]).isEqualTo("0af7651916cd43dd8448eb211c80319c")
	}

	@Test
	fun `should return TransientFailure when broker NACKs`() {
		val correlationSlot = slot<CorrelationData>()

		every {
			rabbitTemplate.send(any<String>(), any<String>(), any<Message>(), capture(correlationSlot))
		} answers {
			val cd = correlationSlot.captured
			val confirm = CorrelationData.Confirm(false, "Queue quota exceeded")
			cd.future.complete(confirm)
		}

		val result = binder.publish(sampleRecord)

		assertThat(result).isInstanceOf(PublishResult.TransientFailure::class.java)
		val failure = result as PublishResult.TransientFailure
		assertThat(failure.cause.message).contains("Queue quota exceeded")
	}

	@Test
	fun `should return TransientFailure when confirm times out`() {
		val correlationSlot = slot<CorrelationData>()

		every {
			rabbitTemplate.send(any<String>(), any<String>(), any<Message>(), capture(correlationSlot))
		} answers {
			val cd = correlationSlot.captured
			cd.future.completeExceptionally(TimeoutException("Timeout"))
		}

		val result = binder.publish(sampleRecord)
		assertThat(result).isInstanceOf(PublishResult.TransientFailure::class.java)
	}
}
