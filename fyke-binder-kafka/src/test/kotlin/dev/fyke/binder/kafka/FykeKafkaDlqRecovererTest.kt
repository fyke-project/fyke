package dev.fyke.binder.kafka

import dev.fyke.core.model.DlqRecord
import dev.fyke.core.model.DlqSource
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.core.telemetry.FykeTelemetry
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.charset.StandardCharsets

class FykeKafkaDlqRecovererTest {

	private val outboxStore = mockk<OutboxStore>(relaxed = true)
	private val telemetry = mockk<FykeTelemetry>(relaxed = true)
	private val recoverer = FykeKafkaDlqRecoverer(outboxStore, telemetry)

	@Test
	fun `should capture consumer poison pill in fyke_dlq`() {
		val record = ConsumerRecord("orders.topic", 1, 42L, "order-999", "{\"error\":\"inventory_unavailable\"}".toByteArray(StandardCharsets.UTF_8))
		record.headers().apply {
			add(RecordHeader("x-fyke-business-key", "order-999".toByteArray(StandardCharsets.UTF_8)))
			add(RecordHeader("x-fyke-type", "OrderPlaced".toByteArray(StandardCharsets.UTF_8)))
			add(RecordHeader("x-fyke-partition-key", "tenant-beta".toByteArray(StandardCharsets.UTF_8)))
		}

		val cause = RuntimeException("Poison pill schema validation failure")
		val dlqSlot = slot<DlqRecord>()
		every { outboxStore.saveDlq(capture(dlqSlot)) } returns Unit

		recoverer.accept(record, cause)

		verify(exactly = 1) { outboxStore.saveDlq(any()) }
		verify(exactly = 1) { telemetry.recordDlqMessage() }

		val captured = dlqSlot.captured
		assertThat(captured.source).isEqualTo(DlqSource.CONSUMER)
		assertThat(captured.businessKey).isEqualTo("order-999")
		assertThat(captured.type).isEqualTo("OrderPlaced")
		assertThat(captured.partitionKey).isEqualTo("tenant-beta")
		assertThat(captured.destination).isEqualTo("orders.topic")
		assertThat(captured.target).isEqualTo("1:42")
		assertThat(captured.consumer).isEqualTo("kafka-orders.topic")
		assertThat(captured.reason).contains("Poison pill schema validation failure")
		assertThat(String(captured.payload, StandardCharsets.UTF_8)).isEqualTo("{\"error\":\"inventory_unavailable\"}")
	}
}
