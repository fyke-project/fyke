package dev.fyke.core.inbox

import dev.fyke.core.model.DlqRecord
import dev.fyke.core.model.DlqSource
import dev.fyke.core.model.InboxRecord
import dev.fyke.core.partition.PartitionLocker
import dev.fyke.core.poller.AbstractPollerEngine
import dev.fyke.core.poller.BackoffPolicy
import dev.fyke.core.poller.NotificationSource
import dev.fyke.core.serializer.FykePayloadSerializer
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.core.telemetry.FykeTelemetry
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
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
	partitionLocker: PartitionLocker,
	notificationSources: List<NotificationSource> = emptyList(),
	private val serializer: FykePayloadSerializer,
	private val telemetry: FykeTelemetry,
	dataSource: DataSource,
	batchSize: Int = 50,
	leaseDuration: Duration = Duration.ofMinutes(1),
	private val maxAttempts: Int = 5,
	initialBackoffMs: Long = 1000,
	backoffMultiplier: Double = 2.0,
	concurrency: Int = 4
) : AbstractPollerEngine<InboxRecord>(
	partitionLocker = partitionLocker,
	notificationSources = notificationSources,
	dataSource = dataSource,
	batchSize = batchSize,
	leaseDuration = leaseDuration,
	concurrency = concurrency,
	threadPrefix = "fyke-inbox"
) {

	private val listeners = ConcurrentHashMap<String, MutableList<InboxListenerRegistration>>()
	private val backoffPolicy = BackoffPolicy(
		initialBackoffMs = initialBackoffMs,
		backoffMultiplier = backoffMultiplier,
		withJitter = true
	)

	fun registerListener(destination: String, bean: Any, method: Method) {
		val paramTypes = method.parameterTypes
		val targetType = if (paramTypes.isNotEmpty()) paramTypes[0] else Any::class.java
		method.isAccessible = true
		listeners.computeIfAbsent(destination) { mutableListOf() }
			.add(InboxListenerRegistration(destination, bean, method, targetType))
		log.info(
			"Fyke: Registered @FykeListener for destination '{}' -> {}.{}()",
			destination, bean.javaClass.simpleName, method.name
		)
	}

	override fun findPendingPartitions(): List<String> {
		return inboxStore.findPendingPartitions()
	}

	override fun claimBatch(partition: String): List<InboxRecord> {
		return inboxStore.claimBatch(batchSize, leaseDuration, partition)
	}

	override fun processRecord(record: InboxRecord) {
		val destinationListeners = listeners[record.destination]
		if (destinationListeners.isNullOrEmpty()) {
			log.warn(
				"Fyke: No @FykeListener registered for destination '{}'; skipping record {}",
				record.destination, record.id
			)
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
				log.error(
					"Fyke: Fatal deserialization error for record {} on destination '{}'; moving to DLQ: {}",
					record.id, record.destination, e.message
				)
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
			targetType == InboxRecord::class.java -> null
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
			val backoffMs = backoffPolicy.calculate(nextAttempt)
			val nextAttemptAt = Instant.now().plusMillis(backoffMs)
			log.warn(
				"Fyke: Transient failure processing inbox record {} (attempt {}/{}); retrying at {}: {}",
				record.id, nextAttempt, maxAttempts, nextAttemptAt, cause.message
			)
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
