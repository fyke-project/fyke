package dev.fyke.exporter.controlplane.heartbeat

import com.google.protobuf.Timestamp
import dev.fyke.controlplane.v1.HeartbeatReport
import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.outbox.OutboxStore
import org.slf4j.LoggerFactory
import java.time.Instant

class ControlPlaneHeartbeatReporter(
	private val outboxStore: OutboxStore?,
	private val inboxStore: InboxStore?
) {
	private val log = LoggerFactory.getLogger(javaClass)

	fun buildHeartbeatReport(): HeartbeatReport {
		val now = Instant.now()
		val builder = HeartbeatReport.newBuilder()
			.setTimestamp(
				Timestamp.newBuilder()
					.setSeconds(now.epochSecond)
					.setNanos(now.nano)
					.build()
			)

		var dbConnected = true

		try {
			val outboxPending = outboxStore?.countPending() ?: 0
			val dlqUnresolved = outboxStore?.countUnreplayedDlq() ?: 0
			builder.setFykeOutboxPendingCount(outboxPending)
			builder.setFykeDlqUnresolvedCount(dlqUnresolved)
		} catch (e: Exception) {
			log.warn("Failed to query outbox counts for heartbeat: {}", e.message)
			dbConnected = false
		}

		try {
			val inboxPending = inboxStore?.countPending() ?: 0
			builder.setFykeInboxPendingCount(inboxPending)
		} catch (e: Exception) {
			log.warn("Failed to query inbox counts for heartbeat: {}", e.message)
			dbConnected = false
		}

		builder.setIsDbConnected(dbConnected)
		builder.setIsBrokerConnected(true)

		return builder.build()
	}
}
