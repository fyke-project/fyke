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
	val notificationSources: List<NotificationSource>,
	private val dataSource: DataSource,
	val batchSize: Int,
	val leaseDuration: Duration,
	val concurrency: Int = 1,
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
		log.trace("Fyke: Triggering poll for {} (running={}, polling={})", threadPrefix, running.get(), polling.get())
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
		log.trace("Fyke: {} pollOnce cycle started", threadPrefix)
		val partitions = findPendingPartitions()
		if (partitions.isEmpty()) {
			log.trace("Fyke: {} pollOnce found 0 pending partitions", threadPrefix)
			onNoPendingPartitions()
			return 0
		}
		log.debug("Fyke: {} pollOnce found {} pending partition(s): {}", threadPrefix, partitions.size, partitions)

		val pool = workerPool
		val total = if (pool != null && !pool.isShutdown) {
			val futures = partitions.map { partition ->
				pool.submit(Callable { processPartition(partition) })
			}
			var count = 0
			for (future in futures) {
				try {
					count += future.get(leaseDuration.toMillis(), TimeUnit.MILLISECONDS)
				} catch (e: Exception) {
					log.warn("Fyke: Error processing partition task: {}", e.message)
				}
			}
			onBatchCompleted()
			count
		} else {
			var count = 0
			for (partition in partitions) {
				if (executor != null && !running.get()) break
				count += processPartition(partition)
			}
			onBatchCompleted()
			count
		}
		log.trace("Fyke: {} pollOnce finished, total records processed: {}", threadPrefix, total)
		return total
	}

	private fun processPartition(partition: String): Int {
		var conn: Connection? = null
		var processed = 0
		try {
			conn = dataSource.connection
			conn.autoCommit = false

			log.trace("Fyke: {} attempting lock for partition '{}'", threadPrefix, partition)
			val locked = partitionLocker.tryLock(partition, conn)
			if (!locked) {
				log.trace("Fyke: {} partition '{}' is locked by another worker; skipping", threadPrefix, partition)
				return 0
			}

			try {
				val batch = claimBatch(partition)
				if (batch.isEmpty()) {
					log.trace("Fyke: {} claimed 0 records for partition '{}'", threadPrefix, partition)
					return 0
				}
				log.debug("Fyke: {} claimed {} record(s) for partition '{}'", threadPrefix, batch.size, partition)

				for (record in batch) {
					if (executor != null && !running.get()) break
					log.trace("Fyke: {} processing record in partition '{}'", threadPrefix, partition)
					processRecord(record)
					processed++
				}
			} finally {
				log.trace("Fyke: {} releasing lock for partition '{}' (processed={}) and committing", threadPrefix, partition, processed)
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
