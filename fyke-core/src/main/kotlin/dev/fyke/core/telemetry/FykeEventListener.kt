package dev.fyke.core.telemetry

import dev.fyke.core.model.DlqRecord
import dev.fyke.core.model.InboxRecord
import dev.fyke.core.model.InboxStatus
import dev.fyke.core.model.OutboxRecord
import dev.fyke.core.model.OutboxStatus

/**
 * SPI listener for receiving domain event lifecycle transitions in-JVM (ADR D-001, D-002).
 *
 * Implementations (such as control plane telemetry exporters) receive immutable snapshots
 * of outbox, inbox, and DLQ state transitions.
 *
 * Implementations must execute non-blocking operations to prevent delaying application transactions.
 */
interface FykeEventListener {

	/**
	 * Invoked when an outbox record is captured in the database transaction (initial status NEW).
	 */
	fun onOutboxCreated(record: OutboxRecord) {}

	/**
	 * Invoked when an outbox record transitions to a new status (e.g. DISPATCHING, PUBLISHED, DEAD).
	 */
	fun onOutboxStatusChanged(
		record: OutboxRecord,
		oldStatus: OutboxStatus,
		newStatus: OutboxStatus,
		errorReason: String? = null
	) {}

	/**
	 * Invoked when an incoming message is ingested into the transactional inbox table (initial status NEW).
	 */
	fun onInboxReceived(record: InboxRecord) {}

	/**
	 * Invoked when an inbox record transitions to a new status (e.g. PROCESSING, COMPLETED, DEAD, retrying NEW).
	 */
	fun onInboxStatusChanged(
		record: InboxRecord,
		oldStatus: InboxStatus,
		newStatus: InboxStatus,
		errorReason: String? = null
	) {}

	/**
	 * Invoked when a message is routed or captured directly into `fyke_dlq`.
	 */
	fun onDlqCaptured(record: DlqRecord) {}
}
