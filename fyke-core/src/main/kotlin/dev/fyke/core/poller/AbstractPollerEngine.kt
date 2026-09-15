package dev.fyke.core.poller

import dev.fyke.core.partition.PartitionLocker
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import javax.sql.DataSource

/**
 * Base coordinator for partition-locked, batch-claimed message processing.
 *
 * Implements:
 * - Lifecycle management with NotificationSource integration.
 * - Debounced polling and non-blocking drain loops.
 * - Per-partition mutual exclusion via [PartitionLocker].
 * - Optional worker pool concurrency across distinct partitions.
 */
abstract class AbstractPollerEngine<T : Any>(
	private val partitionLocker: PartitionLocker,
	private val notificationSources: List<NotificationSource>,
	private val dataSource: DataSource,
	protected val batchSize: Int,
	protected val leaseDuration: Duration,
	protected val concurrency: Int = 1,
	private val threadPrefix: String = "fyke-poller"
) {

	protected val log = LoggerFactory.getLogger(javaClass)
	private val running = AtomicBoolean(false)
	private val polling = AtomicBoolean(false)
	private var executor: ExecutorService? = null
	private var workerPool: ExecutorService? = null

	open fun start() {
		if (!running.compareAndSet(false, true)) return

		if (concurrency > 1) {
			workerPool = Executors.newFixedThreadPool(concurrency) { r ->
				Thread(r, "$threadPrefix-worker").apply { isDaemon = true }
			}
		}

		val exec = Executors.newSingleThreadExecutor { r ->
			Thread(r, threadPrefix).apply { isDaemon = true }
		}
		executor = exec

		notificationSources.forEach { source ->
			source.start {
				triggerPoll()
			}
		}

		// Initial catch-up sweep
		triggerPoll()
		log.info(
			"Fyke: {} started (batchSize={}, leaseDuration={}, concurrency={})",
			threadPrefix, batchSize, leaseDuration, concurrency
		)
	}

	open fun stop() {
		if (running.compareAndSet(true, false)) {
			notificationSources.forEach { it.stop() }
			executor?.shutdownNow()
			workerPool?.shutdown()
			try {
				if (workerPool?.awaitTermination(3, TimeUnit.SECONDS) == false) {
					workerPool?.shutdownNow()
				}
			} catch (_: InterruptedException) {
				workerPool?.shutdownNow()
			}
			executor = null
			workerPool = null
			log.info("Fyke: {} stopped", threadPrefix)
		}
	}

	fun triggerPoll() {
		if (!running.get()) return
		executor?.execute {
			if (polling.compareAndSet(false, true)) {
				try {
					var processed: Int
					do {
						processed = pollOnce()
					} while (processed > 0 && running.get())
				} catch (e: Exception) {
					log.error("Fyke: Unexpected error during {} poll loop: {}", threadPrefix, e.message, e)
				} finally {
					polling.set(false)
				}
			}
		}
	}

	fun pollOnce(): Int {
		val partitions = findPendingPartitions()
		if (partitions.isEmpty()) {
			onNoPendingPartitions()
			return 0
		}

		val pool = workerPool
		return if (pool != null && !pool.isShutdown) {
			val futures = partitions.map { partition ->
				pool.submit(Callable { processPartition(partition) })
			}
			var total = 0
			for (future in futures) {
				try {
					total += future.get(leaseDuration.toMillis(), TimeUnit.MILLISECONDS)
				} catch (e: Exception) {
					log.warn("Fyke: Error processing partition task: {}", e.message)
				}
			}
			onBatchCompleted()
			total
		} else {
			var total = 0
			for (partition in partitions) {
				if (executor != null && !running.get()) break
				total += processPartition(partition)
			}
			onBatchCompleted()
			total
		}
	}

	private fun processPartition(partition: String): Int {
		var conn: Connection? = null
		var processed = 0
		try {
			conn = dataSource.connection
			conn.autoCommit = false

			val locked = partitionLocker.tryLock(partition, conn)
			if (!locked) {
				return 0
			}

			try {
				val batch = claimBatch(partition)
				if (batch.isEmpty()) {
					return 0
				}

				for (record in batch) {
					if (executor != null && !running.get()) break
					processRecord(record)
					processed++
				}
			} finally {
				partitionLocker.unlock(partition, conn)
				conn.commit()
			}
		} catch (e: Exception) {
			log.warn("Fyke: Error processing partition '{}': {}", partition, e.message)
			try {
				conn?.rollback()
			} catch (_: Exception) {}
		} finally {
			try {
				conn?.close()
			} catch (_: Exception) {}
		}
		return processed
	}

	fun isRunning(): Boolean = running.get()

	/**
	 * Discovers distinct partition keys that currently have pending messages ready for processing.
	 */
	protected abstract fun findPendingPartitions(): List<String>

	/**
	 * Claims a batch of records for the given locked partition.
	 */
	protected abstract fun claimBatch(partition: String): List<T>

	/**
	 * Dispatches and processes an individual record.
	 */
	protected abstract fun processRecord(record: T)

	/**
	 * Callback hook executed when no partitions have pending messages.
	 */
	protected open fun onNoPendingPartitions() {}

	/**
	 * Callback hook executed after completing a round of partition processing.
	 */
	protected open fun onBatchCompleted() {}
}
