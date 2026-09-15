package dev.fyke.core.inbox

import dev.fyke.core.model.DlqRecord
import dev.fyke.core.model.DlqSource
import dev.fyke.core.model.InboxRecord
import dev.fyke.core.partition.PartitionLocker
import dev.fyke.core.serializer.FykePayloadSerializer
import dev.fyke.core.store.OutboxStore
import dev.fyke.core.telemetry.FykeTelemetry
import org.slf4j.LoggerFactory
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.sql.Connection
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import javax.sql.DataSource

/**
 * Metadata representing a registered consumer handler method.
 */
data class InboxListenerRegistration(
	val destination: String,
	val bean: Any,
	val method: Method,
	val targetType: Class<*>
)

/**
 * Background dispatch engine for processing incoming events stored in `fyke_inbox`.
 *
 * Implements partition claiming, deserialization, invocation of @FykeListener handlers,
 * exponential retry backoff, and dead-letter routing.
 */
class InboxPollerEngine(
	private val inboxStore: InboxStore,
	private val outboxStore: OutboxStore,
	private val partitionLocker: PartitionLocker,
	private val serializer: FykePayloadSerializer,
	private val telemetry: FykeTelemetry,
	private val dataSource: DataSource,
	private val batchSize: Int = 50,
	private val leaseDuration: Duration = Duration.ofMinutes(1),
	private val maxAttempts: Int = 5,
	private val initialBackoffMs: Long = 1000,
	private val backoffMultiplier: Double = 2.0,
	private val pollIntervalMs: Long = 500,
	private val concurrency: Int = 4
) {

	private val log = LoggerFactory.getLogger(javaClass)
	private val running = AtomicBoolean(false)
	private val listeners = ConcurrentHashMap<String, MutableList<InboxListenerRegistration>>()
	private var scheduler: ScheduledExecutorService? = null
	private var workerPool: java.util.concurrent.ExecutorService? = null

	fun registerListener(destination: String, bean: Any, method: Method) {
		val paramTypes = method.parameterTypes
		val targetType = if (paramTypes.isNotEmpty()) paramTypes[0] else Any::class.java
		method.isAccessible = true
		listeners.computeIfAbsent(destination) { mutableListOf() }
			.add(InboxListenerRegistration(destination, bean, method, targetType))
		log.info("Fyke: Registered @FykeListener for destination '{}' -> {}.{}()",
			destination, bean.javaClass.simpleName, method.name)
	}

	fun start() {
		if (running.compareAndSet(false, true)) {
			if (concurrency > 1) {
				workerPool = Executors.newFixedThreadPool(concurrency) { r ->
					Thread(r, "fyke-inbox-worker").apply { isDaemon = true }
				}
			}
			val exec = Executors.newSingleThreadScheduledExecutor { r ->
				Thread(r, "fyke-inbox-poller").apply { isDaemon = true }
			}
			scheduler = exec
			exec.scheduleWithFixedDelay(
				{ pollOnce() },
				100,
				pollIntervalMs,
				TimeUnit.MILLISECONDS
			)
			log.info("Fyke: InboxPollerEngine started with poll interval {} ms and concurrency {}", pollIntervalMs, concurrency)
		}
	}

	fun stop() {
		if (running.compareAndSet(true, false)) {
			scheduler?.shutdown()
			workerPool?.shutdown()
			try {
				if (scheduler?.awaitTermination(3, TimeUnit.SECONDS) == false) {
					scheduler?.shutdownNow()
				}
				if (workerPool?.awaitTermination(3, TimeUnit.SECONDS) == false) {
					workerPool?.shutdownNow()
				}
			} catch (_: InterruptedException) {
				scheduler?.shutdownNow()
				workerPool?.shutdownNow()
			}
			log.info("Fyke: InboxPollerEngine stopped")
		}
	}

	fun triggerPoll() {
		if (running.get()) {
			scheduler?.execute { pollOnce() }
		}
	}

	fun pollOnce(): Int {
		val partitions = inboxStore.findPendingPartitions()
		if (partitions.isEmpty()) {
			return 0
		}

		val pool = workerPool
		return if (pool != null && !pool.isShutdown) {
			val futures = partitions.map { partition ->
				pool.submit(java.util.concurrent.Callable { processPartition(partition) })
			}
			var total = 0
			for (future in futures) {
				try {
					total += future.get(leaseDuration.toMillis(), TimeUnit.MILLISECONDS)
				} catch (e: Exception) {
					log.warn("Fyke: Error processing partition task: {}", e.message)
				}
			}
			total
		} else {
			var total = 0
			for (partition in partitions) {
				if (scheduler != null && !running.get()) break
				total += processPartition(partition)
			}
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
				val batch = inboxStore.claimBatch(batchSize, leaseDuration, partition)
				if (batch.isEmpty()) {
					return 0
				}

				for (record in batch) {
					if (scheduler != null && !running.get()) break
					processRecord(record)
					processed++
				}
			} finally {
				partitionLocker.unlock(partition, conn)
				conn.commit()
			}
		} catch (e: Exception) {
			log.warn("Fyke: Error processing inbox partition '{}': {}", partition, e.message)
			try { conn?.rollback() } catch (_: Exception) {}
		} finally {
			try { conn?.close() } catch (_: Exception) {}
		}
		return processed
	}

	private fun processRecord(record: InboxRecord) {
		val destinationListeners = listeners[record.destination]
		if (destinationListeners.isNullOrEmpty()) {
			log.warn("Fyke: No @FykeListener registered for destination '{}'; skipping record {}",
				record.destination, record.id)
			return
		}

		for (registration in destinationListeners) {
			val startTime = System.currentTimeMillis()

			// Step 1: Deserialization
			val deserialized: Any?
			try {
				deserialized = deserializePayload(record.payload, registration.targetType)
			} catch (e: Throwable) {
				// Fatal poison pill: cannot deserialize payload
				log.error("Fyke: Fatal deserialization error for record {} on destination '{}'; moving to DLQ: {}",
					record.id, record.destination, e.message)
				inboxStore.markDead(record.id)
				saveToDlq(record, e)
				telemetry.recordDlqMessage()
				return
			}

			// Step 2: Method invocation
			try {
				if (registration.method.parameterCount == 0) {
					registration.method.invoke(registration.bean)
				} else {
					registration.method.invoke(registration.bean, deserialized)
				}
				val durationMs = System.currentTimeMillis() - startTime
				inboxStore.markCompleted(record.id, Instant.now())
				log.debug("Fyke: Successfully processed inbox record {} in {} ms", record.id, durationMs)
			} catch (e: Throwable) {
				val actualCause = if (e is InvocationTargetException && e.targetException != null) e.targetException else e
				handleProcessingFailure(record, actualCause)
			}
		}
	}

	private fun deserializePayload(payload: ByteArray, targetType: Class<*>): Any? {
		return when {
			targetType == ByteArray::class.java -> payload
			targetType == String::class.java -> String(payload, Charsets.UTF_8)
			targetType == InboxRecord::class.java -> null // Handled if someone requests the record itself
			else -> serializer.deserialize(payload, targetType)
		}
	}

	private fun handleProcessingFailure(record: InboxRecord, cause: Throwable) {
		val nextAttempt = record.attempts + 1

		if (isFatal(cause) || nextAttempt >= maxAttempts) {
			val reason = if (isFatal(cause)) {
				"Fatal exception in consumer: ${cause.javaClass.name}: ${cause.message}"
			} else {
				"Exhausted max retries ($maxAttempts): ${cause.javaClass.name}: ${cause.message}"
			}
			log.error("Fyke: Inbox record {} marked DEAD; moving to DLQ: {}", record.id, reason)
			inboxStore.markDead(record.id)
			saveToDlq(record, cause)
			telemetry.recordDlqMessage()
		} else {
			val backoffMs = (initialBackoffMs * Math.pow(backoffMultiplier, (record.attempts).toDouble())).toLong()
			val nextAttemptAt = Instant.now().plusMillis(backoffMs)
			log.warn("Fyke: Transient failure processing inbox record {} (attempt {}/{}); retrying at {}: {}",
				record.id, nextAttempt, maxAttempts, nextAttemptAt, cause.message)
			inboxStore.markRetry(record.id, nextAttempt, nextAttemptAt)
		}
	}

	private fun isFatal(cause: Throwable): Boolean {
		val name = cause.javaClass.name
		return name.contains("MessageConversionException") ||
			name.contains("JsonParseException") ||
			name.contains("JsonMappingException") ||
			name.contains("DeserializationException") ||
			cause is IllegalArgumentException
	}

	private fun saveToDlq(record: InboxRecord, cause: Throwable) {
		val stackTrace = cause.stackTraceToString().take(4000)
		val dlqRecord = DlqRecord(
			id = UUID.randomUUID(),
			source = DlqSource.CONSUMER,
			inboxId = record.id,
			partitionKey = record.partitionKey,
			type = record.type,
			destination = record.destination,
			target = record.target,
			businessKey = record.businessKey,
			contentType = record.contentType,
			payload = record.payload,
			headers = record.headers,
			reason = "${cause.javaClass.name}: ${cause.message}\n$stackTrace",
			consumer = record.consumer,
			receivedAt = Instant.now()
		)
		try {
			outboxStore.saveDlq(dlqRecord)
		} catch (e: Exception) {
			log.error("Fyke: Failed to save consumer DLQ entry for record {}: {}", record.id, e.message, e)
		}
	}
}
