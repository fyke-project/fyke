package dev.fyke.core.retention

import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.outbox.OutboxStore
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Background retention and housekeeping worker (R10, D-015).
 *
 * Purges expired PUBLISHED outbox rows, COMPLETED inbox rows, and replayed DLQ rows
 * in bounded batches to avoid table bloat.
 */
class RetentionCleaner(
	private val outboxStore: OutboxStore,
	private val inboxStore: InboxStore? = null,
	private val outboxRetentionEnabled: Boolean = true,
	private val outboxTtl: Duration = Duration.ofDays(7),
	private val inboxRetentionEnabled: Boolean = true,
	private val inboxTtl: Duration = Duration.ofDays(14),
	private val dlqRetentionEnabled: Boolean = true,
	private val dlqTtl: Duration = Duration.ofDays(30),
	private val purgeInterval: Duration = Duration.ofHours(1),
	private val batchSize: Int = 1000
) {
	private val log = LoggerFactory.getLogger(javaClass)
	private val running = AtomicBoolean(false)
	private var scheduler: ScheduledExecutorService? = null

	fun start() {
		if (!running.compareAndSet(false, true)) return

		val s = Executors.newSingleThreadScheduledExecutor { r ->
			Thread(r, "fyke-retention-cleaner").apply { isDaemon = true }
		}
		scheduler = s
		s.scheduleWithFixedDelay({
			if (running.get()) {
				clean()
			}
		}, purgeInterval.toMillis(), purgeInterval.toMillis(), TimeUnit.MILLISECONDS)

		log.info(
			"Fyke: Retention cleaner started (outboxEnabled={}, outboxTtl={}, inboxEnabled={}, inboxTtl={}, dlqEnabled={}, dlqTtl={}, purgeInterval={}, batchSize={})",
			outboxRetentionEnabled, outboxTtl, inboxRetentionEnabled, inboxTtl, dlqRetentionEnabled, dlqTtl, purgeInterval, batchSize
		)
	}

	fun stop() {
		if (running.compareAndSet(true, false)) {
			scheduler?.shutdownNow()
			scheduler = null
			log.info("Fyke: Retention cleaner stopped")
		}
	}

	fun clean(): CleanResult {
		var totalOutboxPurged = 0
		var totalInboxPurged = 0
		var totalDlqPurged = 0

		try {
			if (outboxRetentionEnabled) {
				val outboxCutoff = Instant.now().minus(outboxTtl)
				do {
					val purged = outboxStore.purgePublished(outboxCutoff, batchSize)
					totalOutboxPurged += purged
				} while (purged == batchSize && running.get())
			}

			if (inboxRetentionEnabled && inboxStore != null) {
				val inboxCutoff = Instant.now().minus(inboxTtl)
				do {
					val purged = inboxStore.purgeCompleted(inboxCutoff, batchSize)
					totalInboxPurged += purged
				} while (purged == batchSize && running.get())
			}

			if (dlqRetentionEnabled) {
				val dlqCutoff = Instant.now().minus(dlqTtl)
				do {
					val purged = outboxStore.purgeDlq(dlqCutoff, batchSize)
					totalDlqPurged += purged
				} while (purged == batchSize && running.get())
			}

			if (totalOutboxPurged > 0 || totalInboxPurged > 0 || totalDlqPurged > 0) {
				log.info(
					"Fyke: Retention cleanup completed: {} published outbox rows purged, {} completed inbox rows purged, {} DLQ rows purged",
					totalOutboxPurged, totalInboxPurged, totalDlqPurged
				)
			}
		} catch (e: Exception) {
			log.warn("Fyke: Error during retention cleanup run: {}", e.message)
		}

		return CleanResult(totalOutboxPurged, totalDlqPurged, totalInboxPurged)
	}

	data class CleanResult(val outboxPurged: Int, val dlqPurged: Int, val inboxPurged: Int = 0)
}
