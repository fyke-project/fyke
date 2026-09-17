package dev.fyke.exporter.controlplane.telemetry

import dev.fyke.controlplane.v1.EventChannel
import dev.fyke.controlplane.v1.EventMetadataRecord
import dev.fyke.controlplane.v1.EventStatus
import dev.fyke.core.model.OutboxRecord
import dev.fyke.core.model.OutboxStatus
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID

class TelemetryRingBufferTest {

	@Test
	fun `enqueue and drain within capacity`() {
		val buffer = TelemetryRingBuffer(capacity = 10)

		val record1 = EventMetadataRecord.newBuilder().setEventId("e-1").build()
		val record2 = EventMetadataRecord.newBuilder().setEventId("e-2").build()

		buffer.enqueue(record1)
		buffer.enqueue(record2)

		assertThat(buffer.size()).isEqualTo(2)

		val drained = buffer.drain(5)
		assertThat(drained).hasSize(2)
		assertThat(drained[0].eventId).isEqualTo("e-1")
		assertThat(drained[1].eventId).isEqualTo("e-2")
		assertThat(buffer.size()).isEqualTo(0)
	}

	@Test
	fun `drop oldest when capacity exceeded`() {
		val buffer = TelemetryRingBuffer(capacity = 3)

		for (i in 1..5) {
			buffer.enqueue(EventMetadataRecord.newBuilder().setEventId("e-$i").build())
		}

		assertThat(buffer.size()).isEqualTo(3)
		assertThat(buffer.getDroppedCount()).isEqualTo(2)

		val drained = buffer.drain(10)
		assertThat(drained.map { it.eventId }).containsExactly("e-3", "e-4", "e-5")
	}

	@Test
	fun `recordOutboxEvent sanitizes headers and hashes payload without leaking raw bytes`() {
		val buffer = TelemetryRingBuffer(capacity = 10)

		val id = UUID.randomUUID()
		val rawPayload = "{\"secretCreditCard\":\"4111222233334444\"}".toByteArray()
		val record = OutboxRecord(
			id = id,
			seq = 1L,
			partitionKey = "test-partition",
			type = "PaymentProcessed",
			destination = "payments-exchange",
			target = "payments.usd",
			businessKey = "ACC-987",
			idempotencyKey = "idem-1",
			status = OutboxStatus.PUBLISHED,
			contentType = "application/json",
			payload = rawPayload,
			payloadHash = "hash123",
			headers = mapOf("Authorization" to "secret-token", "TraceId" to "tr-1"),
			correlationId = "corr-1",
			attempts = 1,
			createdAt = Instant.now(),
			updatedAt = Instant.now(),
			publishedAt = Instant.now()
		)

		buffer.recordOutboxEvent(record)

		val drained = buffer.drain(1)
		assertThat(drained).hasSize(1)

		val meta = drained.first()
		assertThat(meta.eventId).isEqualTo(id.toString())
		assertThat(meta.channel).isEqualTo(EventChannel.EVENT_CHANNEL_OUTBOX)
		assertThat(meta.eventType).isEqualTo("PaymentProcessed")
		assertThat(meta.destination).isEqualTo("payments-exchange")
		assertThat(meta.businessKey).isEqualTo("ACC-987")
		assertThat(meta.status).isEqualTo(EventStatus.EVENT_STATUS_PUBLISHED)
		assertThat(meta.payloadSizeBytes).isEqualTo(rawPayload.size.toLong())
		assertThat(meta.payloadSha256).isEqualTo("hash123")
	}
}
