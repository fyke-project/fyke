package dev.fyke.binder.rabbit

import dev.fyke.core.model.DlqRecord
import dev.fyke.core.model.DlqSource
import dev.fyke.core.store.OutboxStore
import dev.fyke.core.telemetry.FykeTelemetry
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageProperties

class FykeRabbitDlqRecovererTest {

	private val outboxStore = mockk<OutboxStore>(relaxed = true)
	private val telemetry = mockk<FykeTelemetry>(relaxed = true)
	private val recoverer = FykeRabbitDlqRecoverer(outboxStore, telemetry)

	@Test
	fun `should capture consumer poison pill in fyke_dlq`() {
		val properties = MessageProperties().apply {
			receivedExchange = "events.exchange"
			receivedRoutingKey = "order.payment.failed"
			consumerQueue = "orders.dlq"
			setHeader("x-fyke-business-key", "order-999")
			setHeader("x-fyke-type", "PaymentFailed")
			setHeader("x-fyke-partition-key", "tenant-alpha")
		}
		val message = Message("{\"error\":\"card_declined\"}".toByteArray(Charsets.UTF_8), properties)
		val cause = RuntimeException("Unrecoverable database failure during payment processing")

		val dlqSlot = slot<DlqRecord>()
		every { outboxStore.saveDlq(capture(dlqSlot)) } returns Unit

		recoverer.recover(message, cause)

		verify(exactly = 1) { outboxStore.saveDlq(any()) }
		verify(exactly = 1) { telemetry.recordDlqMessage() }

		val captured = dlqSlot.captured
		assertThat(captured.source).isEqualTo(DlqSource.CONSUMER)
		assertThat(captured.businessKey).isEqualTo("order-999")
		assertThat(captured.type).isEqualTo("PaymentFailed")
		assertThat(captured.partitionKey).isEqualTo("tenant-alpha")
		assertThat(captured.destination).isEqualTo("events.exchange")
		assertThat(captured.target).isEqualTo("order.payment.failed")
		assertThat(captured.consumer).isEqualTo("orders.dlq")
		assertThat(captured.reason).contains("Unrecoverable database failure")
		assertThat(String(captured.payload, Charsets.UTF_8)).isEqualTo("{\"error\":\"card_declined\"}")
	}
}
