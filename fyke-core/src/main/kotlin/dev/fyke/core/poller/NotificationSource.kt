package dev.fyke.core.poller

import org.postgresql.PGConnection
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import javax.sql.DataSource

/**
 * Wakeup trigger source for the outbox poller engine (R2).
 */
interface NotificationSource {
	fun start(onWakeup: () -> Unit)
	fun stop()
	fun isConnected(): Boolean = true
}

/**
 * Zero-latency PostgreSQL LISTEN/NOTIFY notification channel (R2, D-004, D-016).
 *
 * Uses a dedicated physical connection outside the application's connection pool.
 * Automatically recovers from connection drops and triggers a catch-up poll on reconnect.
 */
class PgNotifyChannel(
	private val connectionSupplier: () -> Connection,
	private val channelName: String = "fyke_events"
) : NotificationSource {

	private val log = LoggerFactory.getLogger(javaClass)
	private val running = AtomicBoolean(false)
	private val connected = AtomicBoolean(false)
	private var thread: Thread? = null

	override fun start(onWakeup: () -> Unit) {
		if (!running.compareAndSet(false, true)) return

		thread = Thread({
			var backoffMs = 1000L
			while (running.get()) {
				var conn: Connection? = null
				try {
					conn = connectionSupplier()
					conn.autoCommit = true
					conn.createStatement().use { stmt ->
						stmt.execute("LISTEN $channelName")
					}
					connected.set(true)
					backoffMs = 1000L
					log.info("Fyke: LISTEN established on Postgres channel '{}'", channelName)

					// Immediate catch-up sweep upon connection
					onWakeup()

					val pgConn = conn.unwrap(PGConnection::class.java)
					while (running.get() && !conn.isClosed) {
						// getNotifications blocks for timeoutMs; 0 means non-blocking
						val notifications = pgConn.getNotifications(1000)
						if (notifications != null && notifications.isNotEmpty()) {
							log.debug(
								"Fyke: Received {} notification(s) on Postgres channel '{}', triggering wakeup",
								notifications.size,
								channelName
							)
							onWakeup()
						}
					}
				} catch (e: InterruptedException) {
					break
				} catch (e: Exception) {
					connected.set(false)
					if (running.get()) {
						log.warn("Fyke: Postgres LISTEN connection dropped ({}), reconnecting in {} ms...", e.message, backoffMs)
						try {
							Thread.sleep(backoffMs)
							backoffMs = (backoffMs * 2).coerceAtMost(30000L)
						} catch (ie: InterruptedException) {
							break
						}
					}
				} finally {
					connected.set(false)
					try {
						conn?.close()
					} catch (_: Exception) {}
				}
			}
		}, "fyke-pg-listener").apply {
			isDaemon = true
			start()
		}
	}

	override fun stop() {
		if (running.compareAndSet(true, false)) {
			thread?.interrupt()
			thread = null
			connected.set(false)
		}
	}

	override fun isConnected(): Boolean = connected.get()
}

/**
 * Interval poller fallback for non-Postgres databases or idle safety-net (R2).
 */
class TimerChannel(
	private val initialDelayMs: Long = 1000L,
	private val pollIntervalMs: Long = 1000L
) : NotificationSource {

	private val log = LoggerFactory.getLogger(javaClass)
	private val running = AtomicBoolean(false)
	private var scheduler: ScheduledExecutorService? = null

	override fun start(onWakeup: () -> Unit) {
		if (!running.compareAndSet(false, true)) return

		val s = Executors.newSingleThreadScheduledExecutor { r ->
			Thread(r, "fyke-timer-poller").apply { isDaemon = true }
		}
		scheduler = s
		log.debug("Fyke: TimerChannel started (initialDelay={} ms, pollInterval={} ms)", initialDelayMs, pollIntervalMs)
		s.scheduleWithFixedDelay({
			if (running.get()) {
				log.trace("Fyke: TimerChannel tick triggering wakeup")
				onWakeup()
			}
		}, initialDelayMs, pollIntervalMs, TimeUnit.MILLISECONDS)
	}

	override fun stop() {
		if (running.compareAndSet(true, false)) {
			scheduler?.shutdownNow()
			scheduler = null
			log.debug("Fyke: TimerChannel stopped")
		}
	}
}
