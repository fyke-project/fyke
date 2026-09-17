package dev.fyke.exporter.controlplane.telemetry

import dev.fyke.core.model.DlqRecord
import dev.fyke.core.model.InboxRecord
import dev.fyke.core.model.InboxStatus
import dev.fyke.core.model.OutboxRecord
import dev.fyke.core.model.OutboxStatus
import dev.fyke.core.telemetry.FykeEventListener
import org.slf4j.LoggerFactory

/**
 * Control Plane implementation of [FykeEventListener].
 *
 * Ingests domain event lifecycle transitions into the non-blocking [TelemetryRingBuffer]
 * for upstream batch dispatch to the Fyke Control Plane (ADR D-001, D-002, D-009).
 */
class ControlPlaneEventListener(
	private val ringBuffer: TelemetryRingBuffer
) : FykeEventListener {

	private val log = LoggerFactory.getLogger(javaClass)

	override fun onOutboxCreated(record: OutboxRecord) {
		log.trace("ControlPlaneEventListener: outbox created id={}", record.id)
		ringBuffer.recordOutboxEvent(record)
	}

	override fun onOutboxStatusChanged(
		record: OutboxRecord,
		oldStatus: OutboxStatus,
		newStatus: OutboxStatus,
		errorReason: String?
	) {
		log.trace("ControlPlaneEventListener: outbox status changed id={} ({} -> {})", record.id, oldStatus, newStatus)
		ringBuffer.recordOutboxEvent(record.copy(status = newStatus), errorReason = errorReason)
	}

	override fun onInboxReceived(record: InboxRecord) {
		log.trace("ControlPlaneEventListener: inbox received id={}", record.id)
		ringBuffer.recordInboxEvent(record)
	}

	override fun onInboxStatusChanged(
		record: InboxRecord,
		oldStatus: InboxStatus,
		newStatus: InboxStatus,
		errorReason: String?
	) {
		log.trace("ControlPlaneEventListener: inbox status changed id={} ({} -> {})", record.id, oldStatus, newStatus)
		ringBuffer.recordInboxEvent(record.copy(status = newStatus), errorReason = errorReason)
	}

	override fun onDlqCaptured(record: DlqRecord) {
		log.trace("ControlPlaneEventListener: DLQ captured id={}", record.id)
		ringBuffer.recordDlqEvent(record)
	}
}
