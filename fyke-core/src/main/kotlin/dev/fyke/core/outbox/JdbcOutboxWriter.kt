package dev.fyke.core.outbox

import dev.fyke.core.model.OutboxEvent
import dev.fyke.core.model.OutboxRecord
import dev.fyke.core.partition.PartitionResolver
import dev.fyke.core.serializer.FykePayloadSerializer
import dev.fyke.core.telemetry.FykeTelemetry
import org.slf4j.LoggerFactory
import org.springframework.jdbc.datasource.DataSourceUtils
import org.springframework.transaction.support.TransactionSynchronization
import org.springframework.transaction.support.TransactionSynchronizationManager
import java.time.Instant
import java.util.*
import javax.sql.DataSource

/**
 * JDBC implementation of OutboxWriter participating in the active Spring @Transactional boundary (R1).
 */
class JdbcOutboxWriter(
	private val dataSource: DataSource,
	private val outboxStore: OutboxStore,
	private val serializer: FykePayloadSerializer,
	private val partitionResolver: PartitionResolver,
	private val telemetry: FykeTelemetry,
	private val onCommitWakeup: (() -> Unit)? = null
) : OutboxWriter {

	private val log = LoggerFactory.getLogger(javaClass)

	override fun write(event: OutboxEvent): OutboxRecord {
		val txActive = TransactionSynchronizationManager.isActualTransactionActive()
		val recordId = UUID.randomUUID()
		if (!txActive) {
			log.warn(
				"Fyke: Event '{}' (id={}) captured outside an active Spring @Transactional boundary.",
				event.type,
				recordId
			)
		}

		return telemetry.recordSpan(
			"fyke.outbox.capture", mapOf(
				"business_key" to event.businessKey,
				"type" to event.type,
				"destination" to event.destination,
				"idempotency_key" to event.idempotencyKey
			)
		) { span ->
			val traceId = if (span.spanContext.isValid) span.spanContext.traceId else null
			val payloadBytes = serializer.serialize(event.payload)
			val payloadHash = serializer.computeHash(payloadBytes)
			val partitionKey = event.partitionKey ?: partitionResolver.resolvePartition(event)
			val now = Instant.now()

			val record = OutboxRecord(
				id = recordId,
				partitionKey = partitionKey,
				type = event.type,
				destination = event.destination,
				target = event.target,
				businessKey = event.businessKey,
				idempotencyKey = event.idempotencyKey,
				contentType = serializer.contentType(),
				payload = payloadBytes,
				payloadHash = payloadHash,
				headers = event.headers,
				correlationId = event.correlationId,
				traceId = traceId,
				size = payloadBytes.size,
				createdAt = now,
				updatedAt = now
			)

			log.debug(
				"Fyke: Capturing outbox event '{}' (id={}, businessKey={}, destination={}, partitionKey={})",
				event.type,
				recordId,
				event.businessKey,
				event.destination,
				partitionKey
			)
			log.trace(
				"Fyke: Serialized outbox record id={} (payloadSize={} bytes, hash={})",
				recordId,
				payloadBytes.size,
				payloadHash
			)

			val conn = DataSourceUtils.getConnection(dataSource)
			try {
				outboxStore.save(record)

				// Transactional NOTIFY hint on PostgreSQL
				val isPostgres = conn.metaData.databaseProductName.equals("PostgreSQL", ignoreCase = true)
				if (isPostgres) {
					try {
						conn.createStatement().use { stmt ->
							stmt.execute("SELECT pg_notify('fyke_events', '1')")
						}
						log.trace("Fyke: Sent transactional pg_notify('fyke_events') for record id={}", recordId)
					} catch (e: Exception) {
						log.debug("Failed to execute transactional pg_notify: {}", e.message)
					}
				}

				if (txActive) {
					TransactionSynchronizationManager.registerSynchronization(object : TransactionSynchronization {
						override fun afterCommit() {
							log.trace("Fyke: Executing afterCommit wakeup for record id={}", recordId)
							onCommitWakeup?.invoke()
						}
					})
					log.trace("Fyke: Registered afterCommit synchronization for record id={}", recordId)
				} else {
					log.trace("Fyke: Triggering immediate wakeup for record id={}", recordId)
					onCommitWakeup?.invoke()
				}
			} finally {
				DataSourceUtils.releaseConnection(conn, dataSource)
			}

			record
		}
	}
}
