package dev.fyke.core.inbox

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import dev.fyke.core.model.FykeRecordSummary
import dev.fyke.core.model.InboxRecord
import dev.fyke.core.model.InboxStatus
import dev.fyke.core.model.OrderingMode
import org.postgresql.util.PGobject
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import java.sql.Timestamp
import java.time.Duration
import java.time.Instant
import java.util.UUID
import javax.sql.DataSource

class JdbcInboxStore(
	private val dataSource: DataSource,
	private val objectMapper: ObjectMapper = ObjectMapper()
) : InboxStore {

	private val log = LoggerFactory.getLogger(javaClass)
	private val jdbcTemplate = JdbcTemplate(dataSource)

	private val inboxRowMapper = RowMapper<InboxRecord> { rs, _ ->
		InboxRecord(
			id = UUID.fromString(rs.getString("id")),
			seq = rs.getLong("seq"),
			partitionKey = rs.getString("partition_key"),
			type = rs.getString("type"),
			destination = rs.getString("destination"),
			target = rs.getString("target"),
			businessKey = rs.getString("business_key"),
			messageId = rs.getString("message_id"),
			status = InboxStatus.valueOf(rs.getString("status")),
			contentType = rs.getString("content_type"),
			payload = rs.getBytes("payload"),
			headers = parseHeaders(rs.getString("headers")),
			consumer = rs.getString("consumer"),
			ordering = OrderingMode.valueOf(rs.getString("ordering")),
			createdAt = rs.getTimestamp("created_at").toInstant(),
			updatedAt = rs.getTimestamp("updated_at").toInstant(),
			completedAt = rs.getTimestamp("completed_at")?.toInstant(),
			attempts = rs.getInt("attempts"),
			nextAttemptAt = rs.getTimestamp("next_attempt_at")?.toInstant(),
			leaseExpiresAt = rs.getTimestamp("lease_expires_at")?.toInstant()
		)
	}

	override fun save(record: InboxRecord): Boolean {
		if (!record.messageId.isNullOrBlank()) {
			val existing = jdbcTemplate.queryForObject(
				"SELECT COUNT(*) FROM fyke_inbox WHERE message_id = ?",
				Long::class.java,
				record.messageId
			) ?: 0L
			if (existing > 0) {
				log.info("Fyke: Duplicate inbox messageId '{}' detected; skipping insert", record.messageId)
				return false
			}
		}

		val sql = """
			INSERT INTO fyke_inbox (
				id, partition_key, type, destination, target, business_key, message_id,
				status, content_type, payload, headers, consumer, ordering,
				created_at, updated_at, attempts
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		""".trimIndent()

		jdbcTemplate.update { conn ->
			val ps = conn.prepareStatement(sql)
			ps.setObject(1, record.id)
			ps.setString(2, record.partitionKey)
			ps.setString(3, record.type)
			ps.setString(4, record.destination)
			ps.setString(5, record.target)
			ps.setString(6, record.businessKey)
			ps.setString(7, record.messageId)
			ps.setString(8, record.status.name)
			ps.setString(9, record.contentType)
			ps.setBytes(10, record.payload)
			setJsonOrString(ps, 11, record.headers?.let { objectMapper.writeValueAsString(it) }, conn)
			ps.setString(12, record.consumer)
			ps.setString(13, record.ordering.name)
			ps.setTimestamp(14, Timestamp.from(record.createdAt))
			ps.setTimestamp(15, Timestamp.from(record.updatedAt))
			ps.setInt(16, record.attempts)
			ps
		}
		return true
	}

	override fun findById(id: UUID): InboxRecord? {
		val sql = "SELECT * FROM fyke_inbox WHERE id = ?"
		return jdbcTemplate.query(sql, inboxRowMapper, id).firstOrNull()
	}

	override fun findPendingPartitions(): List<String> {
		val sql = "SELECT DISTINCT partition_key FROM fyke_inbox WHERE status IN ('NEW', 'PROCESSING')"
		return jdbcTemplate.query(sql) { rs, _ -> rs.getString("partition_key") }
	}

	override fun claimBatch(batchSize: Int, leaseDuration: Duration, partitionKey: String): List<InboxRecord> {
		val now = Instant.now()
		val leaseExpiresAt = now.plus(leaseDuration)

		val selectSql = """
			SELECT o.* FROM fyke_inbox o
			WHERE o.partition_key = ?
			AND o.status IN ('NEW', 'PROCESSING')
			AND (o.next_attempt_at IS NULL OR o.next_attempt_at <= ?)
			AND (o.status = 'NEW' OR o.lease_expires_at <= ?)
			AND (
				o.ordering = 'LEAPFROG'
				OR NOT EXISTS (
					SELECT 1 FROM fyke_inbox prev
					WHERE prev.partition_key = o.partition_key
					AND prev.status IN ('NEW', 'PROCESSING')
					AND prev.seq < o.seq
				)
			)
			ORDER BY o.seq ASC
			LIMIT ?
			FOR UPDATE SKIP LOCKED
		""".trimIndent()

		val claimed = jdbcTemplate.query(
			selectSql,
			inboxRowMapper,
			partitionKey,
			Timestamp.from(now),
			Timestamp.from(now),
			batchSize
		)
		if (claimed.isEmpty()) return emptyList()

		val updateSql = """
			UPDATE fyke_inbox
			SET status = 'PROCESSING', lease_expires_at = ?, updated_at = ?
			WHERE id = ?
		""".trimIndent()

		jdbcTemplate.batchUpdate(updateSql, claimed.map { record ->
			arrayOf(Timestamp.from(leaseExpiresAt), Timestamp.from(now), record.id)
		})

		return claimed.map { it.copy(status = InboxStatus.PROCESSING, leaseExpiresAt = leaseExpiresAt) }
	}

	override fun markCompleted(id: UUID, completedAt: Instant) {
		val sql = "UPDATE fyke_inbox SET status = 'COMPLETED', completed_at = ?, updated_at = ? WHERE id = ?"
		jdbcTemplate.update(sql, Timestamp.from(completedAt), Timestamp.from(Instant.now()), id)
	}

	override fun markRetry(id: UUID, attempts: Int, nextAttemptAt: Instant) {
		val sql = "UPDATE fyke_inbox SET status = 'NEW', attempts = ?, next_attempt_at = ?, updated_at = ? WHERE id = ?"
		jdbcTemplate.update(sql, attempts, Timestamp.from(nextAttemptAt), Timestamp.from(Instant.now()), id)
	}

	override fun markDead(id: UUID) {
		val sql = "UPDATE fyke_inbox SET status = 'DEAD', updated_at = ? WHERE id = ?"
		jdbcTemplate.update(sql, Timestamp.from(Instant.now()), id)
	}

	override fun retryNow(id: UUID): Boolean {
		val now = Instant.now()
		val sql = "UPDATE fyke_inbox SET status = 'NEW', next_attempt_at = ?, updated_at = ? WHERE id = ? AND status IN ('NEW', 'PROCESSING')"
		return jdbcTemplate.update(sql, Timestamp.from(now), Timestamp.from(now), id) > 0
	}

	override fun searchByBusinessKey(businessKey: String): List<FykeRecordSummary> {
		val sql = """
			SELECT id, 'INBOX' as source, type, business_key, status, destination, target, created_at as timestamp, attempts, null as reason
			FROM fyke_inbox
			WHERE business_key = ?
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
		}, businessKey)
	}

	override fun purgeCompleted(cutoff: Instant, batchSize: Int): Int {
		val sql = """
			DELETE FROM fyke_inbox
			WHERE id IN (
				SELECT id FROM fyke_inbox
				WHERE status = 'COMPLETED' AND completed_at < ?
				LIMIT ?
			)
		""".trimIndent()
		return jdbcTemplate.update(sql, Timestamp.from(cutoff), batchSize)
	}

	override fun countPending(): Long {
		val sql = "SELECT COUNT(*) FROM fyke_inbox WHERE status IN ('NEW', 'PROCESSING')"
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
