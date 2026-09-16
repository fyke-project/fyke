package dev.fyke.starter

import dev.fyke.core.inbox.InboxPollerEngine
import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.model.FykeRecordSummary
import dev.fyke.core.model.OutboxEvent
import dev.fyke.core.model.OutboxRecord
import dev.fyke.core.outbox.OutboxPollerEngine
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.core.outbox.OutboxWriter
import org.slf4j.LoggerFactory
import java.util.UUID

/**
 * Public static facade for interacting with Fyke from application domain code (R1).
 */
object Fyke {

	private val log = LoggerFactory.getLogger(Fyke::class.java)

	@Volatile
	private var writer: OutboxWriter? = null

	@Volatile
	private var outboxPoller: OutboxPollerEngine? = null

	@Volatile
	private var store: OutboxStore? = null

	@Volatile
	private var inboxStore: InboxStore? = null

	@Volatile
	private var inboxPoller: InboxPollerEngine? = null

	@JvmStatic
	fun initialize(
		writer: OutboxWriter,
		outboxPoller: OutboxPollerEngine,
		store: OutboxStore,
		inboxStore: InboxStore? = null,
		inboxPoller: InboxPollerEngine? = null
	) {
		this.writer = writer
		this.outboxPoller = outboxPoller
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
		log.debug(
			"Fyke.send: Dispatching event '{}' for destination '{}' (businessKey='{}')",
			event.type,
			event.destination,
			event.businessKey
		)
		log.trace(
			"Fyke.send: Event details: payloadType={}, target={}, partitionKey={}",
			event.payload.javaClass.simpleName,
			event.target,
			event.partitionKey
		)
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
		partitionKey: String? = null,
		target: String? = null,
		headers: Map<String, String>? = null,
		correlationId: String? = null
	): OutboxRecord {
		return send(
			OutboxEvent(
				type = type,
				destination = destination,
				businessKey = businessKey,
				payload = payload,
				partitionKey = partitionKey,
				target = target,
				headers = headers,
				correlationId = correlationId
			)
		)
	}

	/**
	 * Manually triggers replay of a dead-lettered or stuck event by ID in the application JVM (R6).
	 */
	@JvmStatic
	fun replay(id: UUID): Boolean {
		val p = outboxPoller ?: error("Fyke is not initialized. Ensure Spring application context has started.")
		log.debug("Fyke.replay: Requesting replay for record id={}", id)
		return p.replay(id)
	}

	/**
	 * Symmetrical alias for [replay] to explicitly distinguish outbox replay from inbox retry.
	 */
	@JvmStatic
	fun replayOutbox(id: UUID): Boolean = replay(id)

	/**
	 * Immediately unblocks a retrying or stuck inbox record and triggers poller execution.
	 */
	@JvmStatic
	fun retryInbox(id: UUID): Boolean {
		val s = inboxStore ?: error("Fyke inbox is not initialized.")
		log.debug("Fyke.retryInbox: Requesting immediate retry for inbox record id={}", id)
		val unblocked = s.retryNow(id)
		if (unblocked) {
			inboxPoller?.triggerPoll()
		}
		return unblocked
	}

	/**
	 * Searches outbox, DLQ, and inbox records by domain business key (R7).
	 */
	@JvmStatic
	fun searchByBusinessKey(businessKey: String): List<FykeRecordSummary> {
		val s = store ?: error("Fyke is not initialized. Ensure Spring application context has started.")
		log.debug("Fyke.searchByBusinessKey: Searching records for businessKey='{}'", businessKey)
		val outboxAndDlq = s.searchByBusinessKey(businessKey)
		val inboxRecords = inboxStore?.searchByBusinessKey(businessKey) ?: emptyList()
		return (outboxAndDlq + inboxRecords).sortedBy { it.timestamp }
	}

	@JvmSynthetic
	internal fun reset() {
		writer = null
		outboxPoller = null
		store = null
		inboxStore = null
		inboxPoller = null
	}
}
