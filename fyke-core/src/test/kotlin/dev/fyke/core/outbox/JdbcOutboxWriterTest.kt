package dev.fyke.core.outbox

import dev.fyke.core.model.OutboxEvent
import dev.fyke.core.partition.PartitionResolver
import dev.fyke.core.serializer.JacksonFykePayloadSerializer
import dev.fyke.core.telemetry.FykeTelemetry
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.Statement
import javax.sql.DataSource

class JdbcOutboxWriterTest {

	private val dataSource = mockk<DataSource>()
	private val connection = mockk<Connection>()
	private val metaData = mockk<DatabaseMetaData>()
	private val statement = mockk<Statement>()
	private val outboxStore = mockk<OutboxStore>(relaxed = true)
	private val partitionResolver = mockk<PartitionResolver>()
	private val serializer = JacksonFykePayloadSerializer()

	private lateinit var spanExporter: InMemorySpanExporter
	private lateinit var openTelemetry: OpenTelemetry
	private lateinit var telemetry: FykeTelemetry

	@BeforeEach
	fun setUp() {
		spanExporter = InMemorySpanExporter.create()
		val tracerProvider = SdkTracerProvider.builder()
			.addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
			.build()

		openTelemetry = OpenTelemetrySdk.builder()
			.setTracerProvider(tracerProvider)
			.build()

		telemetry = FykeTelemetry(openTelemetry)

		every { dataSource.connection } returns connection
		every { connection.metaData } returns metaData
		every { metaData.databaseProductName } returns "PostgreSQL"
		every { connection.createStatement() } returns statement
		every { statement.execute(any()) } returns true
		every { statement.close() } returns Unit
		every { connection.close() } returns Unit

		every { partitionResolver.resolvePartition(any()) } returns "part-1"
	}

	@AfterEach
	fun tearDown() {
		spanExporter.reset()
	}

	@Test
	fun `should auto-populate traceId from active parent span`() {
		val writer = JdbcOutboxWriter(
			dataSource = dataSource,
			outboxStore = outboxStore,
			serializer = serializer,
			partitionResolver = partitionResolver,
			telemetry = telemetry
		)

		val parentTracer = openTelemetry.getTracer("test")
		val parentSpan = parentTracer.spanBuilder("http.request").startSpan()
		val scope = parentSpan.makeCurrent()

		val event = OutboxEvent(
			type = "OrderCreated",
			destination = "orders",
			businessKey = "ord-1",
			payload = mapOf("amount" to 100),
			correlationId = "corr-123"
		)

		val record = try {
			writer.write(event)
		} finally {
			scope.close()
			parentSpan.end()
		}

		assertThat(record.traceId).isNotNull()
		assertThat(record.traceId).isEqualTo(parentSpan.spanContext.traceId)
		assertThat(record.correlationId).isEqualTo("corr-123")
	}

	@Test
	fun `should auto-generate traceId during capture span when no parent span exists`() {
		val writer = JdbcOutboxWriter(
			dataSource = dataSource,
			outboxStore = outboxStore,
			serializer = serializer,
			partitionResolver = partitionResolver,
			telemetry = telemetry
		)

		val event = OutboxEvent(
			type = "OrderCreated",
			destination = "orders",
			businessKey = "ord-2",
			payload = mapOf("amount" to 200)
		)

		val record = writer.write(event)

		assertThat(record.traceId).isNotNull()
		val spans = spanExporter.finishedSpanItems
		assertThat(spans).hasSize(1)
		assertThat(record.traceId).isEqualTo(spans.first().traceId)
	}

	@Test
	fun `should leave traceId null when OpenTelemetry is noop`() {
		val writer = JdbcOutboxWriter(
			dataSource = dataSource,
			outboxStore = outboxStore,
			serializer = serializer,
			partitionResolver = partitionResolver,
			telemetry = FykeTelemetry(OpenTelemetry.noop())
		)

		val event = OutboxEvent(
			type = "OrderCreated",
			destination = "orders",
			businessKey = "ord-3",
			payload = mapOf("amount" to 300)
		)

		val record = writer.write(event)
		assertThat(record.traceId).isNull()
	}
}
