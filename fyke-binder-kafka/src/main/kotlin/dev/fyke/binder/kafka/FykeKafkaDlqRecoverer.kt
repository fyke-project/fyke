package dev.fyke.binder.kafka

import dev.fyke.core.model.DlqRecord
import dev.fyke.core.model.DlqSource
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.core.telemetry.FykeTelemetry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.listener.ConsumerRecordRecoverer
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID

/**
 * Spring Kafka ConsumerRecordRecoverer capturing poison pills into `fyke_dlq` (R4).
 */
class FykeKafkaDlqRecoverer(
	private val outboxStore: OutboxStore,
	private val telemetry: FykeTelemetry
) : ConsumerRecordRecoverer {

	private val log = LoggerFactory.getLogger(javaClass)

	override fun accept(record: ConsumerRecord<*, *>, exception: Exception) {
		val headers = mutableMapOf<String, String>()
		record.headers().forEach { header ->
			headers[header.key()] = String(header.value(), StandardCharsets.UTF_8)
		}

		val businessKey = headers["x-fyke-business-key"]
			?: headers["business_key"]
			?: record.key()?.toString()
			?: "unknown"

		val type = headers["x-fyke-type"]
			?: headers["type"]
			?: record.topic()

		val partitionKey = headers["x-fyke-partition-key"]
			?: record.key()?.toString()
			?: "default"

		val contentType = headers["x-fyke-content-type"]
			?: "application/octet-stream"

		val destination = record.topic()
		val target = "${record.partition()}:${record.offset()}"

		val outboxId = headers["x-fyke-event-id"]?.let {
			try { UUID.fromString(it) } catch (_: Exception) { null }
		}

		val rawPayload = when (val value = record.value()) {
			is ByteArray -> value
			is String -> value.toByteArray(StandardCharsets.UTF_8)
			else -> value?.toString()?.toByteArray(StandardCharsets.UTF_8) ?: ByteArray(0)
		}

		val cause = exception.cause ?: exception
		val stackTrace = cause.stackTraceToString().take(4000)

		val dlqRecord = DlqRecord(
			id = UUID.randomUUID(),
			source = DlqSource.CONSUMER,
			outboxId = outboxId,
			partitionKey = partitionKey,
			type = type,
			destination = destination,
			target = target,
			businessKey = businessKey,
			contentType = contentType,
			payload = rawPayload,
			headers = headers,
			reason = "${cause.javaClass.name}: ${cause.message}\n$stackTrace",
			consumer = "kafka-${record.topic()}",
			receivedAt = Instant.now()
		)

		log.debug(
			"Fyke: Recovering Kafka poison message from topic '{}' (businessKey={}, type={})",
			record.topic(),
			businessKey,
			type
		)
		log.trace("Fyke: Kafka poison message headers: {}", headers)

		try {
			outboxStore.saveDlq(dlqRecord)
			telemetry.recordDlqMessage()
			telemetry.notifyDlqCaptured(dlqRecord)
			log.error(
				"Fyke: Consumer poison pill captured in fyke_dlq (id={}, businessKey={}, topic={}, partition={}, offset={}): {}",
				dlqRecord.id,
				businessKey,
				record.topic(),
				record.partition(),
				record.offset(),
				cause.message
			)
		} catch (e: Exception) {
			log.error("Fyke: Failed to save Kafka consumer DLQ record: {}", e.message, e)
		}
	}
}
