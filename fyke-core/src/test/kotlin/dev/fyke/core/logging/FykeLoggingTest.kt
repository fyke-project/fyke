package dev.fyke.core.logging

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import dev.fyke.core.inbox.DefaultConsumerPartitionResolver
import dev.fyke.core.model.OutboxEvent
import dev.fyke.core.outbox.JdbcOutboxWriter
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.core.partition.PartitionResolver
import dev.fyke.core.partition.SingleWorkerPartitionLocker
import dev.fyke.core.serializer.JacksonFykePayloadSerializer
import dev.fyke.core.telemetry.FykeTelemetry
import io.mockk.every
import io.mockk.mockk
import io.opentelemetry.api.OpenTelemetry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.Statement
import javax.sql.DataSource

class FykeLoggingTest {

	private val rootFykeLogger = LoggerFactory.getLogger("dev.fyke") as Logger
	private lateinit var listAppender: ListAppender<ILoggingEvent>
	private var originalLevel: Level? = null

	@BeforeEach
	fun setUp() {
		originalLevel = rootFykeLogger.level
		rootFykeLogger.level = Level.TRACE
		listAppender = ListAppender<ILoggingEvent>()
		listAppender.start()
		rootFykeLogger.addAppender(listAppender)
	}

	@AfterEach
	fun tearDown() {
		rootFykeLogger.detachAppender(listAppender)
		rootFykeLogger.level = originalLevel
	}

	@Test
	fun `JdbcOutboxWriter logs debug on event capture and trace on serialization`() {
		val dataSource = mockk<DataSource>()
		val connection = mockk<Connection>()
		val metaData = mockk<DatabaseMetaData>()
		val statement = mockk<Statement>()
		val outboxStore = mockk<OutboxStore>(relaxed = true)
		val partitionResolver = PartitionResolver { "part-test" }
		val serializer = JacksonFykePayloadSerializer()
		val telemetry = FykeTelemetry(OpenTelemetry.noop())

		every { dataSource.connection } returns connection
		every { connection.metaData } returns metaData
		every { metaData.databaseProductName } returns "PostgreSQL"
		every { connection.createStatement() } returns statement
		every { statement.execute(any()) } returns true
		every { statement.close() } returns Unit
		every { connection.close() } returns Unit

		val writer = JdbcOutboxWriter(
			dataSource = dataSource,
			outboxStore = outboxStore,
			serializer = serializer,
			partitionResolver = partitionResolver,
			telemetry = telemetry
		)

		val event = OutboxEvent(
			type = "UserRegistered",
			destination = "users.topic",
			businessKey = "user-123",
			payload = mapOf("username" to "johndoe")
		)

		writer.write(event)

		val logMessages = listAppender.list.map { it.formattedMessage }
		assertThat(logMessages).anyMatch {
			it.contains("Capturing outbox event 'UserRegistered'") &&
				it.contains("businessKey=user-123") &&
				it.contains("destination=users.topic") &&
				it.contains("partitionKey=part-test")
		}
		assertThat(logMessages).anyMatch {
			it.contains("Serialized outbox record") && it.contains("payloadSize=")
		}
		assertThat(logMessages).anyMatch {
			it.contains("Sent transactional pg_notify('fyke_events')")
		}
	}

	@Test
	fun `DefaultConsumerPartitionResolver logs trace messages for each tier`() {
		val resolver = DefaultConsumerPartitionResolver()

		// Tier 1
		val payload = """{"tenantId":"acme"}""".toByteArray()
		val p1 = resolver.resolve(emptyMap(), payload, "tenantId")
		assertThat(p1).isEqualTo("acme")

		// Tier 2
		val p2 = resolver.resolve(mapOf("x-fyke-business-key" to "biz-999"), ByteArray(0), null)
		assertThat(p2).isEqualTo("biz-999")

		// Tier 3
		val p3 = resolver.resolve(emptyMap(), ByteArray(0), null)
		assertThat(p3).isEqualTo("default")

		val logMessages = listAppender.list.map { it.formattedMessage }
		assertThat(logMessages).anyMatch { it.contains("Resolved consumer partition 'acme' from property 'tenantId'") }
		assertThat(logMessages).anyMatch { it.contains("Resolved consumer partition 'biz-999' from header") }
		assertThat(logMessages).anyMatch { it.contains("Resolved consumer default partition 'default'") }
	}

	@Test
	fun `SingleWorkerPartitionLocker logs trace messages on lock and unlock`() {
		val locker = SingleWorkerPartitionLocker()
		val connection = mockk<Connection>()

		val acquired = locker.tryLock("partition-alpha", connection)
		assertThat(acquired).isTrue()

		locker.unlock("partition-alpha", connection)

		val logMessages = listAppender.list.map { it.formattedMessage }
		assertThat(logMessages).anyMatch { it.contains("SingleWorkerPartitionLocker.tryLock for partition 'partition-alpha' -> true") }
		assertThat(logMessages).anyMatch { it.contains("SingleWorkerPartitionLocker.unlock for partition 'partition-alpha'") }
	}

	@Test
	fun `FykeTelemetry logs trace on metrics and spans`() {
		val telemetry = FykeTelemetry(OpenTelemetry.noop())

		telemetry.recordPublished(15)
		telemetry.recordPublishFailure()
		telemetry.recordDlqMessage()
		telemetry.updateBacklogDepth(7)
		telemetry.recordSpan("test.operation", mapOf("key" to "value")) { "result" }

		val logMessages = listAppender.list.map { it.formattedMessage }
		assertThat(logMessages).anyMatch { it.contains("Telemetry counter published +1, latency=15 ms") }
		assertThat(logMessages).anyMatch { it.contains("Telemetry counter publishFailure +1") }
		assertThat(logMessages).anyMatch { it.contains("Telemetry counter dlqGrowth +1") }
		assertThat(logMessages).anyMatch { it.contains("Telemetry gauge backlogDepth=7") }
		assertThat(logMessages).anyMatch { it.contains("Recording telemetry span 'test.operation'") }
	}
}
