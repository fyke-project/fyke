package dev.fyke.core.store

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import dev.fyke.core.model.*
import org.postgresql.util.PGobject
import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.util.*
import javax.sql.DataSource

class JdbcOutboxStore(
	private val dataSource: DataSource,
	private val objectMapper: ObjectMapper = ObjectMapper()
) : OutboxStore {

	private val log = LoggerFactory.getLogger(javaClass)
	private val jdbcTemplate = JdbcTemplate(dataSource)

	private val outboxRowMapper = RowMapper<OutboxRecord> { rs, _ ->
		OutboxRecord(
			id = UUID.fromString(rs.getString("id")),
			seq = rs.getLong("seq"),
			partitionKey = rs.getString("partition_key"),
			type = rs.getString("type"),
			destination = rs.getString("destination"),
			target = rs.getString("target"),
			businessKey = rs.getString("business_key"),
			idempotencyKey = rs.getString("idempotency_key"),
			status = OutboxStatus.valueOf(rs.getString("status")),
			contentType = rs.getString("content_type"),
			payload = rs.getBytes("payload"),
			payloadHash = rs.getString("payload_hash"),
			headers = parseHeaders(rs.getString("headers")),
			correlationId = rs.getString("correlation_id"),
			traceId = rs.getString("trace_id"),
			size = rs.getInt("size"),
			createdAt = rs.getTimestamp("created_at").toInstant(),
			updatedAt = rs.getTimestamp("updated_at").toInstant(),
			publishedAt = rs.getTimestamp("published_at")?.toInstant(),
			attempts = rs.getInt("attempts"),
			nextAttemptAt = rs.getTimestamp("next_attempt_at")?.toInstant(),
			leaseExpiresAt = rs.getTimestamp("lease_expires_at")?.toInstant()
		)
	}

	private val dlqRowMapper = RowMapper<DlqRecord> { rs, _ ->
		DlqRecord(
			id = UUID.fromString(rs.getString("id")),
			source = DlqSource.valueOf(rs.getString("source")),
			outboxId = rs.getString("outbox_id")?.let { UUID.fromString(it) },
			partitionKey = rs.getString("partition_key"),
			type = rs.getString("type"),
			destination = rs.getString("destination"),
			target = rs.getString("target"),
			businessKey = rs.getString("business_key"),
			contentType = rs.getString("content_type"),
			payload = rs.getBytes("payload"),
			headers = parseHeaders(rs.getString("headers")),
			reason = rs.getString("reason"),
			consumer = rs.getString("consumer"),
			receivedAt = rs.getTimestamp("received_at").toInstant(),
			replayedAt = rs.getTimestamp("replayed_at")?.toInstant()
		)
	}

	override fun save(record: OutboxRecord) {
		val sql = """
			INSERT INTO fyke_outbox (
				id, partition_key, type, destination, target, business_key, idempotency_key,
				status, content_type, payload, payload_hash, headers, correlation_id, trace_id,
				size, created_at, updated_at, attempts, next_attempt_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		""".trimIndent()

		try {
			jdbcTemplate.update { conn ->
				val ps = conn.prepareStatement(sql)
				ps.setObject(1, record.id)
				ps.setString(2, record.partitionKey)
				ps.setString(3, record.type)
				ps.setString(4, record.destination)
				ps.setString(5, record.target)
				ps.setString(6, record.businessKey)
				ps.setString(7, record.idempotencyKey)
				ps.setString(8, record.status.name)
				ps.setString(9, record.contentType)
				ps.setBytes(10, record.payload)
				ps.setString(11, record.payloadHash)
				setJsonOrString(ps, 12, record.headers?.let { objectMapper.writeValueAsString(it) }, conn)
				ps.setString(13, record.correlationId)
				ps.setString(14, record.traceId)
				ps.setInt(15, record.size)
				ps.setTimestamp(16, Timestamp.from(record.createdAt))
				ps.setTimestamp(17, Timestamp.from(record.updatedAt))
				ps.setInt(18, record.attempts)
				ps.setTimestamp(19, record.nextAttemptAt?.let { Timestamp.from(it) })
				ps
			}
		} catch (e: DuplicateKeyException) {
			throw DuplicateIdempotencyKeyException(record.idempotencyKey)
		} catch (e: Exception) {
			if (e.message?.contains("idempotency_key", ignoreCase = true) == true) {
				throw DuplicateIdempotencyKeyException(record.idempotencyKey)
			}
			throw e
		}
	}

	override fun findOutboxById(id: UUID): OutboxRecord? {
		val sql = "SELECT * FROM fyke_outbox WHERE id = ?"
		return jdbcTemplate.query(sql, outboxRowMapper, id).firstOrNull()
	}

	override fun findPendingPartitions(): List<String> {
		val sql = "SELECT DISTINCT partition_key FROM fyke_outbox WHERE status IN ('NEW', 'DISPATCHING')"
		return jdbcTemplate.query(sql) { rs, _ -> rs.getString("partition_key") }
	}

	override fun claimBatch(batchSize: Int, leaseDuration: Duration, partitionKey: String): List<OutboxRecord> {
		val now = Instant.now()
		val leaseExpiresAt = now.plus(leaseDuration)

		val selectSql = """
			SELECT o.* FROM fyke_outbox o
			WHERE o.partition_key = ?
			AND o.status IN ('NEW', 'DISPATCHING')
			AND (o.next_attempt_at IS NULL OR o.next_attempt_at <= ?)
			AND (o.status = 'NEW' OR o.lease_expires_at <= ?)
			AND NOT EXISTS (
				SELECT 1 FROM fyke_outbox prev
				WHERE prev.partition_key = o.partition_key
				AND prev.status IN ('NEW', 'DISPATCHING')
				AND prev.seq < o.seq
			)
			ORDER BY o.seq ASC
			LIMIT ?
			FOR UPDATE SKIP LOCKED
		""".trimIndent()

		val claimed = jdbcTemplate.query(
			selectSql,
			outboxRowMapper,
			partitionKey,
			Timestamp.from(now), Timestamp.from(now), batchSize
		)
		if (claimed.isEmpty()) return emptyList()

		val updateSql = """
			UPDATE fyke_outbox
			SET status = 'DISPATCHING', lease_expires_at = ?, updated_at = ?
			WHERE id = ?
		""".trimIndent()

		jdbcTemplate.batchUpdate(updateSql, claimed.map { record ->
			arrayOf(Timestamp.from(leaseExpiresAt), Timestamp.from(now), record.id)
		})

		return claimed.map { it.copy(status = OutboxStatus.DISPATCHING, leaseExpiresAt = leaseExpiresAt) }
	}

	override fun markPublished(id: UUID, publishedAt: Instant) {
		val sql = "UPDATE fyke_outbox SET status = 'PUBLISHED', published_at = ?, updated_at = ? WHERE id = ?"
		jdbcTemplate.update(sql, Timestamp.from(publishedAt), Timestamp.from(Instant.now()), id)
	}

	override fun markRetry(id: UUID, attempts: Int, nextAttemptAt: Instant) {
		val sql =
			"UPDATE fyke_outbox SET status = 'NEW', attempts = ?, next_attempt_at = ?, updated_at = ? WHERE id = ?"
		jdbcTemplate.update(sql, attempts, Timestamp.from(nextAttemptAt), Timestamp.from(Instant.now()), id)
	}

	override fun markDead(id: UUID, reason: String) {
		val record = findOutboxById(id) ?: return
		val now = Instant.now()

		val updateSql = "UPDATE fyke_outbox SET status = 'DEAD', updated_at = ? WHERE id = ?"
		jdbcTemplate.update(updateSql, Timestamp.from(now), id)

		val dlq = DlqRecord(
			source = DlqSource.OUTBOX,
			outboxId = id,
			partitionKey = record.partitionKey,
			type = record.type,
			destination = record.destination,
			target = record.target,
			businessKey = record.businessKey,
			contentType = record.contentType,
			payload = record.payload,
			headers = record.headers,
			reason = reason,
			consumer = null,
			receivedAt = now
		)
		saveDlq(dlq)
	}

	override fun saveDlq(dlq: DlqRecord) {
		val sql = """
			INSERT INTO fyke_dlq (
				id, source, outbox_id, partition_key, type, destination, target, business_key,
				content_type, payload, headers, reason, consumer, received_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		""".trimIndent()

		jdbcTemplate.update { conn ->
			val ps = conn.prepareStatement(sql)
			ps.setObject(1, dlq.id)
			ps.setString(2, dlq.source.name)
			ps.setObject(3, dlq.outboxId)
			ps.setString(4, dlq.partitionKey)
			ps.setString(5, dlq.type)
			ps.setString(6, dlq.destination)
			ps.setString(7, dlq.target)
			ps.setString(8, dlq.businessKey)
			ps.setString(9, dlq.contentType)
			ps.setBytes(10, dlq.payload)
			setJsonOrString(ps, 11, dlq.headers?.let { objectMapper.writeValueAsString(it) }, conn)
			ps.setString(12, dlq.reason)
			ps.setString(13, dlq.consumer)
			ps.setTimestamp(14, Timestamp.from(dlq.receivedAt))
			ps
		}
	}

	override fun findDlqById(id: UUID): DlqRecord? {
		val sql = "SELECT * FROM fyke_dlq WHERE id = ?"
		return jdbcTemplate.query(sql, dlqRowMapper, id).firstOrNull()
	}

	override fun markDlqReplayed(id: UUID, replayedAt: Instant) {
		val sql = "UPDATE fyke_dlq SET replayed_at = ? WHERE id = ?"
		jdbcTemplate.update(sql, Timestamp.from(replayedAt), id)
	}

	override fun searchByBusinessKey(businessKey: String): List<FykeRecordSummary> {
		val sql = """
			SELECT id, 'OUTBOX' AS source, type, business_key, status, destination, target, created_at AS timestamp, attempts, NULL AS reason
			FROM fyke_outbox WHERE business_key = ?
			UNION ALL
			SELECT id, 'DLQ' AS source, type, business_key, source AS status, destination, target, received_at AS timestamp, 0 AS attempts, reason
			FROM fyke_dlq WHERE business_key = ?
			ORDER BY timestamp ASC
		""".trimIndent()

		return jdbcTemplate.query(sql, { rs, _ ->
			FykeRecordSummary(
				id = UUID.fromString(rs.getString("id")),
				source = rs.getString("source"),
				type = rs.getString("type"),
				businessKey = rs.getString("business_key"),
				status = rs.getString("status"),
				destination = rs.getString("destination"),
				target = rs.getString("target"),
				timestamp = rs.getTimestamp("timestamp").toInstant(),
				attempts = rs.getInt("attempts"),
				reason = rs.getString("reason")
			)
		}, businessKey, businessKey)
	}

	override fun purgePublished(cutoff: Instant, batchSize: Int): Int {
		val sql = """
			DELETE FROM fyke_outbox
			WHERE id IN (
				SELECT id FROM fyke_outbox
				WHERE status = 'PUBLISHED' AND published_at < ?
				LIMIT ?
			)
		""".trimIndent()
		return jdbcTemplate.update(sql, Timestamp.from(cutoff), batchSize)
	}

	override fun purgeDlq(cutoff: Instant, batchSize: Int): Int {
		val sql = """
			DELETE FROM fyke_dlq
			WHERE id IN (
				SELECT id FROM fyke_dlq
				WHERE replayed_at IS NOT NULL AND replayed_at < ?
				LIMIT ?
			)
		""".trimIndent()
		return jdbcTemplate.update(sql, Timestamp.from(cutoff), batchSize)
	}

	override fun countPending(): Long {
		val sql = "SELECT COUNT(*) FROM fyke_outbox WHERE status IN ('NEW', 'DISPATCHING')"
		return jdbcTemplate.queryForObject(sql, Long::class.java) ?: 0L
	}

	private fun setJsonOrString(ps: java.sql.PreparedStatement, index: Int, json: String?, conn: java.sql.Connection) {
		if (json == null) {
			ps.setNull(index, java.sql.Types.OTHER)
			return
		}
		val isPostgres = conn.metaData.databaseProductName.equals("PostgreSQL", ignoreCase = true)
		if (isPostgres) {
			val pgObject = PGobject().apply {
				type = "jsonb"
				value = json
			}
			ps.setObject(index, pgObject)
		} else {
			ps.setString(index, json)
		}
	}

	private fun parseHeaders(json: String?): Map<String, String>? {
		if (json.isNullOrBlank()) return null
		return try {
			objectMapper.readValue(json, object : TypeReference<Map<String, String>>() {})
		} catch (e: Exception) {
			null
		}
	}
}
