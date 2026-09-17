package dev.fyke.exporter.controlplane.telemetry

import com.google.protobuf.Timestamp
import dev.fyke.controlplane.v1.EventChannel
import dev.fyke.controlplane.v1.EventMetadataRecord
import dev.fyke.controlplane.v1.EventStatus
import dev.fyke.core.model.DlqRecord
import dev.fyke.core.model.InboxRecord
import dev.fyke.core.model.InboxStatus
import dev.fyke.core.model.OutboxRecord
import dev.fyke.core.model.OutboxStatus
import dev.fyke.core.telemetry.ClientSideSanitizer
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * High-performance, non-blocking bounded buffer for telemetry records.
 *
 * Implements a drop-oldest overflow policy to guarantee that application domain
 * operations are never blocked under control-plane disconnection or backpressure (ADR D-002, D-009).
 */
class TelemetryRingBuffer(
	private val capacity: Int = 10000,
	private val sanitizer: ClientSideSanitizer = ClientSideSanitizer()
) {
	private val log = LoggerFactory.getLogger(javaClass)

	private val queue = ConcurrentLinkedDeque<EventMetadataRecord>()
	private val currentSize = AtomicInteger(0)
	private val droppedRecordsCount = AtomicLong(0)

	fun enqueue(record: EventMetadataRecord): Boolean {
		// Drop oldest if at or above capacity
		while (currentSize.get() >= capacity) {
			val dropped = queue.pollFirst()
			if (dropped != null) {
				currentSize.decrementAndGet()
				val totalDropped = droppedRecordsCount.incrementAndGet()
				if (totalDropped % 1000 == 1L) {
					log.warn("TelemetryRingBuffer capacity ({}) reached; dropped oldest record. Total dropped: {}", capacity, totalDropped)
				}
			} else {
				break
			}
		}

		queue.addLast(record)
		currentSize.incrementAndGet()
		log.trace("Enqueued telemetry record id={} (bufferSize={})", record.eventId, currentSize.get())
		return true
	}

	fun drain(maxBatchSize: Int): List<EventMetadataRecord> {
		if (queue.isEmpty() || maxBatchSize <= 0) {
			return emptyList()
		}

		val batch = mutableListOf<EventMetadataRecord>()
		while (batch.size < maxBatchSize) {
			val record = queue.pollFirst() ?: break
			currentSize.decrementAndGet()
			batch.add(record)
		}
		return batch
	}

	fun size(): Int = currentSize.get()

	fun getDroppedCount(): Long = droppedRecordsCount.get()

	fun recordOutboxEvent(
		record: OutboxRecord,
		sanitizedHeaders: Map<String, String>? = null,
		errorReason: String? = null
	) {
		val builder = EventMetadataRecord.newBuilder()
			.setEventId(record.id.toString())
			.setChannel(EventChannel.EVENT_CHANNEL_OUTBOX)
			.setEventType(record.type)
			.setDestination(record.destination)
			.setBusinessKey(record.businessKey)
			.setStatus(mapOutboxStatus(record.status))
			.setContentType(record.contentType)
			.setPayloadSizeBytes(record.payload.size.toLong())
			.setPayloadSha256(record.payloadHash)
			.setAttempts(record.attempts)
			.setCreatedAt(toTimestamp(record.createdAt))
			.setUpdatedAt(toTimestamp(record.updatedAt))

		record.target?.let { builder.setTarget(it) }
		builder.setIdempotencyKey(record.idempotencyKey)
		record.correlationId?.let { builder.setCorrelationId(it) }
		errorReason?.let { builder.setErrorReason(it) }

		val headers = sanitizedHeaders ?: sanitizer.sanitizeAttributes(record.headers)
		builder.putAllSanitizedHeaders(headers)

		enqueue(builder.build())
	}

	fun recordInboxEvent(
		record: InboxRecord,
		sanitizedHeaders: Map<String, String>? = null,
		errorReason: String? = null
	) {
		val builder = EventMetadataRecord.newBuilder()
			.setEventId(record.id.toString())
			.setChannel(EventChannel.EVENT_CHANNEL_INBOX)
			.setEventType(record.type)
			.setDestination(record.destination)
			.setBusinessKey(record.businessKey)
			.setStatus(mapInboxStatus(record.status))
			.setContentType(record.contentType)
			.setPayloadSizeBytes(record.payload.size.toLong())
			.setPayloadSha256(sanitizer.hash(String(record.payload, Charsets.UTF_8)))
			.setAttempts(record.attempts)
			.setCreatedAt(toTimestamp(record.createdAt))
			.setUpdatedAt(toTimestamp(record.updatedAt))

		record.target?.let { builder.setTarget(it) }
		record.messageId?.let { builder.setIdempotencyKey(it) }
		errorReason?.let { builder.setErrorReason(it) }

		val headers = sanitizedHeaders ?: sanitizer.sanitizeAttributes(record.headers)
		builder.putAllSanitizedHeaders(headers)

		enqueue(builder.build())
	}

	fun recordDlqEvent(
		record: DlqRecord,
		sanitizedHeaders: Map<String, String>? = null
	) {
		val builder = EventMetadataRecord.newBuilder()
			.setEventId(record.id.toString())
			.setChannel(EventChannel.EVENT_CHANNEL_DLQ)
			.setEventType(record.type)
			.setDestination(record.destination)
			.setBusinessKey(record.businessKey)
			.setStatus(EventStatus.EVENT_STATUS_DEAD)
			.setContentType(record.contentType)
			.setPayloadSizeBytes(record.payload.size.toLong())
			.setPayloadSha256(sanitizer.hash(String(record.payload, Charsets.UTF_8)))
			.setAttempts(0)
			.setErrorReason(record.reason)
			.setCreatedAt(toTimestamp(record.receivedAt))
			.setUpdatedAt(toTimestamp(record.receivedAt))

		record.consumer?.let { builder.setConsumer(it) }
		record.target?.let { builder.setTarget(it) }

		val headers = sanitizedHeaders ?: sanitizer.sanitizeAttributes(record.headers)
		builder.putAllSanitizedHeaders(headers)

		enqueue(builder.build())
	}

	private fun mapOutboxStatus(status: OutboxStatus): EventStatus = when (status) {
		OutboxStatus.NEW -> EventStatus.EVENT_STATUS_NEW
		OutboxStatus.DISPATCHING -> EventStatus.EVENT_STATUS_DISPATCHING
		OutboxStatus.PUBLISHED -> EventStatus.EVENT_STATUS_PUBLISHED
		OutboxStatus.DEAD -> EventStatus.EVENT_STATUS_DEAD
	}

	private fun mapInboxStatus(status: InboxStatus): EventStatus = when (status) {
		InboxStatus.NEW -> EventStatus.EVENT_STATUS_NEW
		InboxStatus.PROCESSING -> EventStatus.EVENT_STATUS_DISPATCHING
		InboxStatus.COMPLETED -> EventStatus.EVENT_STATUS_CONSUMED
		InboxStatus.DEAD -> EventStatus.EVENT_STATUS_DEAD
	}

	private fun toTimestamp(instant: Instant): Timestamp = Timestamp.newBuilder()
		.setSeconds(instant.epochSecond)
		.setNanos(instant.nano)
		.build()
}
