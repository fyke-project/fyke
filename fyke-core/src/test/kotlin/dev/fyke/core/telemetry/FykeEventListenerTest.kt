package dev.fyke.core.telemetry

import dev.fyke.core.model.DlqRecord
import dev.fyke.core.model.DlqSource
import dev.fyke.core.model.InboxRecord
import dev.fyke.core.model.InboxStatus
import dev.fyke.core.model.OutboxRecord
import dev.fyke.core.model.OutboxStatus
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID

class FykeEventListenerTest {

	private class TestEventListener : FykeEventListener {
		val outboxCreated = mutableListOf<OutboxRecord>()
		val outboxStatusChanges = mutableListOf<String>()
		val inboxReceived = mutableListOf<InboxRecord>()
		val inboxStatusChanges = mutableListOf<String>()
		val dlqCaptured = mutableListOf<DlqRecord>()

		override fun onOutboxCreated(record: OutboxRecord) {
			outboxCreated.add(record)
		}

		override fun onOutboxStatusChanged(
			record: OutboxRecord,
			oldStatus: OutboxStatus,
			newStatus: OutboxStatus,
			errorReason: String?
		) {
			outboxStatusChanges.add("${record.id}:$oldStatus->$newStatus:$errorReason")
		}

		override fun onInboxReceived(record: InboxRecord) {
			inboxReceived.add(record)
		}

		override fun onInboxStatusChanged(
			record: InboxRecord,
			oldStatus: InboxStatus,
			newStatus: InboxStatus,
			errorReason: String?
		) {
			inboxStatusChanges.add("${record.id}:$oldStatus->$newStatus:$errorReason")
		}

		override fun onDlqCaptured(record: DlqRecord) {
			dlqCaptured.add(record)
		}
	}

	@Test
	fun `FykeTelemetry dispatches all events to registered listeners and handles listener errors gracefully`() {
		val listener = TestEventListener()
		val faultyListener = object : FykeEventListener {
			override fun onOutboxCreated(record: OutboxRecord) {
				throw RuntimeException("Boom")
			}
		}

		val telemetry = FykeTelemetry(
			sanitizer = ClientSideSanitizer(),
			listeners = listOf(faultyListener, listener)
		)

		val outboxRecord = OutboxRecord(
			id = UUID.randomUUID(),
			type = "OrderCreated",
			destination = "orders.exchange",
			businessKey = "order-123",
			idempotencyKey = "key-123",
			payload = "payload".toByteArray(),
			payloadHash = "hash"
		)

		// 1. Outbox Created (faultyListener throws, but listener still succeeds and doesn't propagate error)
		telemetry.notifyOutboxCreated(outboxRecord)
		assertThat(listener.outboxCreated).containsExactly(outboxRecord)

		// 2. Outbox Status Changed
		telemetry.notifyOutboxStatusChanged(outboxRecord, OutboxStatus.NEW, OutboxStatus.DISPATCHING)
		telemetry.notifyOutboxStatusChanged(
			outboxRecord.copy(status = OutboxStatus.DEAD),
			OutboxStatus.DISPATCHING,
			OutboxStatus.DEAD,
			"Crash"
		)
		assertThat(listener.outboxStatusChanges).containsExactly(
			"${outboxRecord.id}:NEW->DISPATCHING:null",
			"${outboxRecord.id}:DISPATCHING->DEAD:Crash"
		)

		// 3. Inbox Received
		val inboxRecord = InboxRecord(
			id = UUID.randomUUID(),
			type = "PaymentProcessed",
			destination = "payments.queue",
			businessKey = "pay-456",
			payload = "data".toByteArray()
		)
		telemetry.notifyInboxReceived(inboxRecord)
		assertThat(listener.inboxReceived).containsExactly(inboxRecord)

		// 4. Inbox Status Changed
		telemetry.notifyInboxStatusChanged(inboxRecord, InboxStatus.NEW, InboxStatus.PROCESSING)
		telemetry.notifyInboxStatusChanged(
			inboxRecord.copy(status = InboxStatus.COMPLETED),
			InboxStatus.PROCESSING,
			InboxStatus.COMPLETED
		)
		assertThat(listener.inboxStatusChanges).containsExactly(
			"${inboxRecord.id}:NEW->PROCESSING:null",
			"${inboxRecord.id}:PROCESSING->COMPLETED:null"
		)

		// 5. DLQ Captured
		val dlqRecord = DlqRecord(
			id = UUID.randomUUID(),
			source = DlqSource.CONSUMER,
			type = "PaymentProcessed",
			destination = "payments.queue",
			businessKey = "pay-456",
			payload = "data".toByteArray(),
			reason = "Invalid payload"
		)
		telemetry.notifyDlqCaptured(dlqRecord)
		assertThat(listener.dlqCaptured).containsExactly(dlqRecord)
	}
}
