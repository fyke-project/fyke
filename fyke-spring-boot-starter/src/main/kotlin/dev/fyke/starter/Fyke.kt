package dev.fyke.starter

import dev.fyke.core.model.FykeRecordSummary
import dev.fyke.core.model.OutboxEvent
import dev.fyke.core.model.OutboxRecord
import dev.fyke.core.poller.PollerEngine
import dev.fyke.core.store.OutboxStore
import dev.fyke.core.store.OutboxWriter
import java.util.UUID

/**
 * Public static facade for interacting with Fyke from application domain code (R1).
 */
object Fyke {

	@Volatile
	private var writer: OutboxWriter? = null

	@Volatile
	private var poller: PollerEngine? = null

	@Volatile
	private var store: OutboxStore? = null

	@Volatile
	private var inboxStore: dev.fyke.core.inbox.InboxStore? = null

	@Volatile
	private var inboxPoller: dev.fyke.core.inbox.InboxPollerEngine? = null

	@JvmStatic
	fun initialize(
		writer: OutboxWriter,
		poller: PollerEngine,
		store: OutboxStore,
		inboxStore: dev.fyke.core.inbox.InboxStore? = null,
		inboxPoller: dev.fyke.core.inbox.InboxPollerEngine? = null
	) {
		this.writer = writer
		this.poller = poller
		this.store = store
		this.inboxStore = inboxStore
		this.inboxPoller = inboxPoller
	}

	/**
	 * Programmatically registers a domain event for outbox delivery in the active transaction.
	 */
	@JvmStatic
	fun send(event: OutboxEvent): OutboxRecord {
		val w = writer ?: error("Fyke is not initialized. Ensure Spring application context has started.")
		return w.write(event)
	}

	/**
	 * Programmatically registers a domain event with standard parameters.
	 */
	@JvmStatic
	fun send(
		type: String,
		destination: String,
		businessKey: String,
		payload: Any,
		target: String? = null,
		headers: Map<String, String>? = null
	): OutboxRecord {
		return send(
			OutboxEvent(
				type = type,
				destination = destination,
				target = target,
				businessKey = businessKey,
				payload = payload,
				headers = headers
			)
		)
	}

	/**
	 * Replays a dead-letter or outbox event in-JVM via the configured broker binder.
	 *
	 * @param id The UUID of the outbox or DLQ record to replay.
	 * @return true if confirmed by the broker, false otherwise.
	 */
	@JvmStatic
	fun replay(id: UUID): Boolean {
		val p = poller ?: error("Fyke is not initialized.")
		return p.replay(id)
	}

	/**
	 * Immediately resets next_attempt_at for a retrying inbox event, triggering an immediate poll.
	 *
	 * @param id The UUID of the inbox record.
	 * @return true if updated, false otherwise.
	 */
	@JvmStatic
	fun retryInbox(id: UUID): Boolean {
		val s = inboxStore ?: error("Fyke is not initialized with an InboxStore.")
		val updated = s.retryNow(id)
		if (updated) {
			inboxPoller?.triggerPoll()
		}
		return updated
	}

	/**
	 * Searches all outbox, inbox, and DLQ events associated with a specific business key.
	 */
	@JvmStatic
	fun searchByBusinessKey(businessKey: String): List<FykeRecordSummary> {
		val s = store ?: error("Fyke is not initialized.")
		val outboxAndDlq = s.searchByBusinessKey(businessKey)
		val inbox = inboxStore?.searchByBusinessKey(businessKey) ?: emptyList()
		return (outboxAndDlq + inbox).sortedBy { it.timestamp }
	}
}
