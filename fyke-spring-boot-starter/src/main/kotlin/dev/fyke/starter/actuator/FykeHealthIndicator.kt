package dev.fyke.starter.actuator

import dev.fyke.core.inbox.InboxPollerEngine
import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.outbox.OutboxPollerEngine
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.starter.properties.FykeProperties
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.health.contributor.Health
import org.springframework.boot.health.contributor.HealthIndicator

/**
 * HealthIndicator reporting operational health and backlog metrics for Fyke outbox, inbox, and DLQ subsystems.
 *
 * Symmetrically reports status for outbox, inbox, and DLQ.
 * In accordance with Fyke reliability design, broker outages accumulating outbox backlogs do NOT mark
 * the health indicator as DOWN (preventing false-positive Kubernetes pod restarts), but report full backlog details.
 */
class FykeHealthIndicator(
	private val outboxStoreProvider: ObjectProvider<OutboxStore>,
	private val inboxStoreProvider: ObjectProvider<InboxStore>,
	private val outboxPollerEngineProvider: ObjectProvider<OutboxPollerEngine>,
	private val inboxPollerEngineProvider: ObjectProvider<InboxPollerEngine>,
	private val properties: FykeProperties
) : HealthIndicator {

	private val log = LoggerFactory.getLogger(javaClass)

	override fun health(): Health {
		val outboxStore = outboxStoreProvider.ifAvailable
		val inboxStore = inboxStoreProvider.ifAvailable
		val outboxPoller = outboxPollerEngineProvider.ifAvailable
		val inboxPoller = inboxPollerEngineProvider.ifAvailable

		val builder = Health.up()

		// 1. Outbox Subsystem
		if (outboxPoller != null || outboxStore != null) {
			val isRunning = outboxPoller?.isRunning() ?: false
			if (outboxPoller != null && !isRunning) {
				log.warn("Fyke: OutboxPollerEngine is not running; marking health DOWN")
				builder.down().withDetail("outboxFailure", "OutboxPollerEngine is stopped")
			}

			val outboxDetails = mutableMapOf<String, Any>()
			outboxDetails["status"] = if (isRunning) "RUNNING" else "STOPPED"
			outboxDetails["channel"] = properties.outbox.channel.name

			if (outboxStore != null) {
				try {
					outboxDetails["pending"] = outboxStore.countPending()
					outboxDetails["dead"] = outboxStore.countDead()
				} catch (e: Exception) {
					log.error("Fyke: Failed to query outbox store for health: {}", e.message)
					return Health.down(e)
						.withDetail("error", "Failed to query outbox store: ${e.message}")
						.build()
				}
			}
			builder.withDetail("outbox", outboxDetails)
		}

		// 2. Inbox Subsystem
		if (inboxPoller != null || inboxStore != null) {
			val isRunning = inboxPoller?.isRunning() ?: false
			if (inboxPoller != null && !isRunning) {
				log.warn("Fyke: InboxPollerEngine is not running; marking health DOWN")
				builder.down().withDetail("inboxFailure", "InboxPollerEngine is stopped")
			}

			val inboxDetails = mutableMapOf<String, Any>()
			inboxDetails["status"] = if (isRunning) "RUNNING" else "STOPPED"
			inboxDetails["channel"] = properties.inbox.channel.name

			if (inboxPoller != null) {
				inboxDetails["registeredListeners"] = inboxPoller.getRegisteredListeners().size
			}

			if (inboxStore != null) {
				try {
					inboxDetails["pending"] = inboxStore.countPending()
					inboxDetails["dead"] = inboxStore.countDead()
				} catch (e: Exception) {
					log.error("Fyke: Failed to query inbox store for health: {}", e.message)
					return Health.down(e)
						.withDetail("error", "Failed to query inbox store: ${e.message}")
						.build()
				}
			}
			builder.withDetail("inbox", inboxDetails)
		}

		// 3. DLQ Subsystem
		if (outboxStore != null) {
			try {
				val dlqDetails = mapOf(
					"unreplayed" to outboxStore.countUnreplayedDlq()
				)
				builder.withDetail("dlq", dlqDetails)
			} catch (e: Exception) {
				log.error("Fyke: Failed to query DLQ store for health: {}", e.message)
				return Health.down(e)
					.withDetail("error", "Failed to query DLQ store: ${e.message}")
					.build()
			}
		}

		return builder.build()
	}
}
