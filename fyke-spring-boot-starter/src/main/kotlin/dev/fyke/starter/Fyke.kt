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

	@JvmStatic
	fun initialize(writer: OutboxWriter, poller: PollerEngine, store: OutboxStore) {
		this.writer = writer
		this.poller = poller
		this.store = store
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
	 * Searches all outbox and DLQ events associated with a specific business key.
	 */
	@JvmStatic
	fun searchByBusinessKey(businessKey: String): List<FykeRecordSummary> {
		val s = store ?: error("Fyke is not initialized.")
		return s.searchByBusinessKey(businessKey)
	}
}
