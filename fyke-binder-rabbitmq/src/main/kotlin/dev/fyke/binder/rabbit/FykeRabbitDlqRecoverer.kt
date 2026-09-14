package dev.fyke.binder.rabbit

import dev.fyke.core.model.DlqRecord
import dev.fyke.core.model.DlqSource
import dev.fyke.core.store.OutboxStore
import dev.fyke.core.telemetry.FykeTelemetry
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.Message
import org.springframework.amqp.rabbit.retry.MessageRecoverer
import java.time.Instant
import java.util.UUID

/**
 * Spring AMQP MessageRecoverer that captures poison messages directly into `fyke_dlq` (R4).
 */
class FykeRabbitDlqRecoverer(
	private val outboxStore: OutboxStore,
	private val telemetry: FykeTelemetry
) : MessageRecoverer {

	private val log = LoggerFactory.getLogger(javaClass)

	override fun recover(message: Message, cause: Throwable) {
		val properties = message.messageProperties
		val headers = properties.headers.mapValues { it.value?.toString() ?: "" }

		val businessKey = headers["x-fyke-business-key"]
			?: headers["business_key"]
			?: properties.correlationId
			?: properties.messageId
			?: "unknown"

		val type = headers["x-fyke-type"]
			?: headers["type"]
			?: properties.receivedRoutingKey
			?: "amqp.message"

		val partitionKey = headers["x-fyke-partition-key"] ?: "default"
		val destination = properties.receivedExchange?.takeIf { it.isNotBlank() } ?: "default.exchange"
		val target = properties.receivedRoutingKey

		val outboxId = headers["x-fyke-event-id"]?.let {
			try { UUID.fromString(it) } catch (_: Exception) { null }
		}

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
			contentType = properties.contentType,
			payload = message.body,
			headers = headers,
			reason = "${cause.javaClass.name}: ${cause.message}\n$stackTrace",
			consumer = properties.consumerQueue,
			receivedAt = Instant.now()
		)

		try {
			outboxStore.saveDlq(dlqRecord)
			telemetry.recordDlqMessage()
			log.error("Fyke: Consumer poison pill captured in fyke_dlq (id={}, businessKey={}, queue={}): {}",
				dlqRecord.id, businessKey, properties.consumerQueue, cause.message)
		} catch (e: Exception) {
			log.error("Fyke: Failed to save consumer DLQ record: {}", e.message, e)
		}
	}
}
