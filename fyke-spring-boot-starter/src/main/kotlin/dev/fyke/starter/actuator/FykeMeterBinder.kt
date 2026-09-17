package dev.fyke.starter.actuator

import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.outbox.OutboxStore
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.MeterBinder
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider

/**
 * Micrometer [MeterBinder] providing native Spring Boot & Prometheus metric gauges for Fyke outbox, inbox, and DLQ.
 *
 * Exposes:
 * - `fyke.outbox.backlog`: gauge tracking pending outbox rows awaiting dispatch.
 * - `fyke.outbox.dead`: gauge tracking outbox rows marked DEAD (exhausted attempts).
 * - `fyke.inbox.backlog`: gauge tracking pending inbox rows awaiting consumer processing.
 * - `fyke.inbox.dead`: gauge tracking inbox rows marked DEAD.
 * - `fyke.dlq.unreplayed`: gauge tracking dead-letter queue records awaiting human replay.
 */
class FykeMeterBinder(
	private val outboxStoreProvider: ObjectProvider<OutboxStore>,
	private val inboxStoreProvider: ObjectProvider<InboxStore>
) : MeterBinder {

	private val log = LoggerFactory.getLogger(javaClass)

	override fun bindTo(registry: MeterRegistry) {
		val outboxStore = outboxStoreProvider.ifAvailable
		val inboxStore = inboxStoreProvider.ifAvailable

		if (outboxStore != null) {
			Gauge.builder("fyke.outbox.backlog") {
				try {
					outboxStore.countPending().toDouble()
				} catch (e: Exception) {
					log.debug("Failed to record fyke.outbox.backlog metric: {}", e.message)
					0.0
				}
			}
				.description("Current number of pending outbox events awaiting dispatch")
				.register(registry)

			Gauge.builder("fyke.outbox.dead") {
				try {
					outboxStore.countDead().toDouble()
				} catch (e: Exception) {
					log.debug("Failed to record fyke.outbox.dead metric: {}", e.message)
					0.0
				}
			}
				.description("Current number of outbox events marked as DEAD")
				.register(registry)

			Gauge.builder("fyke.dlq.unreplayed") {
				try {
					outboxStore.countUnreplayedDlq().toDouble()
				} catch (e: Exception) {
					log.debug("Failed to record fyke.dlq.unreplayed metric: {}", e.message)
					0.0
				}
			}
				.description("Current number of unreplayed dead-letter records")
				.register(registry)
		}

		if (inboxStore != null) {
			Gauge.builder("fyke.inbox.backlog") {
				try {
					inboxStore.countPending().toDouble()
				} catch (e: Exception) {
					log.debug("Failed to record fyke.inbox.backlog metric: {}", e.message)
					0.0
				}
			}
				.description("Current number of pending inbox events awaiting processing")
				.register(registry)

			Gauge.builder("fyke.inbox.dead") {
				try {
					inboxStore.countDead().toDouble()
				} catch (e: Exception) {
					log.debug("Failed to record fyke.inbox.dead metric: {}", e.message)
					0.0
				}
			}
				.description("Current number of inbox events marked as DEAD")
				.register(registry)
		}

		log.info("Fyke: Registered Micrometer metrics gauges for outbox, inbox, and DLQ")
	}
}
