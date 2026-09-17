package dev.fyke.exporter.controlplane.telemetry

import dev.fyke.controlplane.v1.EventChannel
import dev.fyke.controlplane.v1.EventStatus
import dev.fyke.core.model.DlqRecord
import dev.fyke.core.model.DlqSource
import dev.fyke.core.model.InboxRecord
import dev.fyke.core.model.InboxStatus
import dev.fyke.core.model.OutboxRecord
import dev.fyke.core.model.OutboxStatus
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class ControlPlaneEventListenerTest {

	private lateinit var ringBuffer: TelemetryRingBuffer
	private lateinit var listener: ControlPlaneEventListener

	@BeforeEach
	fun setUp() {
		ringBuffer = TelemetryRingBuffer(capacity = 100)
		listener = ControlPlaneEventListener(ringBuffer)
	}

	@Test
	fun `onOutboxCreated captures record into ring buffer with status NEW`() {
		val record = OutboxRecord(
			id = UUID.randomUUID(),
			type = "OrderCreated",
			destination = "orders.exchange",
			businessKey = "order-101",
			idempotencyKey = "key-101",
			payload = "{}".toByteArray(),
			payloadHash = "hash101"
		)

		listener.onOutboxCreated(record)

		val batch = ringBuffer.drain(10)
		assertThat(batch).hasSize(1)
		val exported = batch[0]
		assertThat(exported.eventId).isEqualTo(record.id.toString())
		assertThat(exported.channel).isEqualTo(EventChannel.EVENT_CHANNEL_OUTBOX)
		assertThat(exported.eventType).isEqualTo("OrderCreated")
		assertThat(exported.businessKey).isEqualTo("order-101")
		assertThat(exported.status).isEqualTo(EventStatus.EVENT_STATUS_NEW)
	}

	@Test
	fun `onOutboxStatusChanged updates status and errorReason`() {
		val record = OutboxRecord(
			id = UUID.randomUUID(),
			type = "OrderCreated",
			destination = "orders.exchange",
			businessKey = "order-102",
			idempotencyKey = "key-102",
			payload = "{}".toByteArray(),
			payloadHash = "hash102"
		)

		listener.onOutboxStatusChanged(
			record = record,
			oldStatus = OutboxStatus.DISPATCHING,
			newStatus = OutboxStatus.DEAD,
			errorReason = "Broker timeout"
		)

		val batch = ringBuffer.drain(10)
		assertThat(batch).hasSize(1)
		val exported = batch[0]
		assertThat(exported.eventId).isEqualTo(record.id.toString())
		assertThat(exported.status).isEqualTo(EventStatus.EVENT_STATUS_DEAD)
		assertThat(exported.errorReason).isEqualTo("Broker timeout")
	}

	@Test
	fun `onInboxReceived and onInboxStatusChanged capture inbox events`() {
		val record = InboxRecord(
			id = UUID.randomUUID(),
			type = "PaymentCaptured",
			destination = "payments.queue",
			businessKey = "payment-201",
			payload = "{}".toByteArray()
		)

		listener.onInboxReceived(record)
		listener.onInboxStatusChanged(record, InboxStatus.NEW, InboxStatus.PROCESSING)
		listener.onInboxStatusChanged(record, InboxStatus.PROCESSING, InboxStatus.COMPLETED)

		val batch = ringBuffer.drain(10)
		assertThat(batch).hasSize(3)
		assertThat(batch[0].channel).isEqualTo(EventChannel.EVENT_CHANNEL_INBOX)
		assertThat(batch[0].status).isEqualTo(EventStatus.EVENT_STATUS_NEW)
		assertThat(batch[1].status).isEqualTo(EventStatus.EVENT_STATUS_DISPATCHING)
		assertThat(batch[2].status).isEqualTo(EventStatus.EVENT_STATUS_CONSUMED)
	}

	@Test
	fun `onDlqCaptured captures dlq event`() {
		val record = DlqRecord(
			id = UUID.randomUUID(),
			source = DlqSource.CONSUMER,
			type = "InventoryReserved",
			destination = "inventory.queue",
			businessKey = "inv-301",
			payload = "{}".toByteArray(),
			reason = "Out of stock exception"
		)

		listener.onDlqCaptured(record)

		val batch = ringBuffer.drain(10)
		assertThat(batch).hasSize(1)
		val exported = batch[0]
		assertThat(exported.channel).isEqualTo(EventChannel.EVENT_CHANNEL_DLQ)
		assertThat(exported.status).isEqualTo(EventStatus.EVENT_STATUS_DEAD)
		assertThat(exported.errorReason).isEqualTo("Out of stock exception")
	}
}
