package dev.fyke.core.inbox

import dev.fyke.core.model.DlqRecord
import dev.fyke.core.model.InboxRecord
import dev.fyke.core.model.InboxStatus
import dev.fyke.core.partition.SingleWorkerPartitionLocker
import dev.fyke.core.serializer.JacksonFykePayloadSerializer
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.core.telemetry.ClientSideSanitizer
import dev.fyke.core.telemetry.FykeTelemetry
import io.mockk.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import javax.sql.DataSource

class InboxPollerEngineTest {

	private val inboxStore = mockk<InboxStore>(relaxed = true)
	private val outboxStore = mockk<OutboxStore>(relaxed = true)
	private val partitionLocker = SingleWorkerPartitionLocker()
	private val serializer = JacksonFykePayloadSerializer()
	private val telemetry = FykeTelemetry(null, ClientSideSanitizer())
	private val dataSource = mockk<DataSource>()
	private val connection = mockk<Connection>(relaxed = true)

	private lateinit var poller: InboxPollerEngine

	data class TestMessage(val message: String)

	class TestConsumer {
		val received = CopyOnWriteArrayList<TestMessage>()
		var shouldFail = false
		var fatalError = false

		@FykeListener(destination = "test.queue")
		fun onMessage(payload: TestMessage) {
			if (fatalError) {
				throw IllegalArgumentException("Deterministic fatal validation failure")
			}
			if (shouldFail) {
				throw RuntimeException("Simulated transient failure")
			}
			received.add(payload)
		}
	}

	@BeforeEach
	fun setUp() {
		every { dataSource.connection } returns connection

		poller = InboxPollerEngine(
			inboxStore = inboxStore,
			outboxStore = outboxStore,
			partitionLocker = partitionLocker,
			serializer = serializer,
			telemetry = telemetry,
			dataSource = dataSource,
			batchSize = 10,
			leaseDuration = Duration.ofSeconds(30),
			maxAttempts = 3,
			initialBackoffMs = 50,
			backoffMultiplier = 2.0
		)
	}

	@Test
	fun `should deserialize payload and invoke registered listener successfully`() {
		val consumer = TestConsumer()
		val method = consumer.javaClass.getMethod("onMessage", TestMessage::class.java)
		poller.registerListener("test.queue", consumer, method)

		val record = InboxRecord(
			id = UUID.randomUUID(),
			partitionKey = "part-1",
			type = "TestMessage",
			destination = "test.queue",
			businessKey = "biz-1",
			payload = """{"message": "Hello Fyke!"}""".toByteArray()
		)

		every { inboxStore.findPendingPartitions() } returns listOf("part-1")
		every { inboxStore.claimBatch(any(), any(), "part-1") } returns listOf(record)

		poller.pollOnce()

		assertThat(consumer.received).hasSize(1)
		assertThat(consumer.received.first().message).isEqualTo("Hello Fyke!")
		verify(exactly = 1) { inboxStore.markCompleted(record.id, any()) }
	}

	@Test
	fun `should immediately dead-letter fatal deserialization error without retries`() {
		val consumer = TestConsumer()
		val method = consumer.javaClass.getMethod("onMessage", TestMessage::class.java)
		poller.registerListener("test.queue", consumer, method)

		// Malformed JSON that fails deserialization
		val record = InboxRecord(
			id = UUID.randomUUID(),
			partitionKey = "part-1",
			type = "TestMessage",
			destination = "test.queue",
			businessKey = "biz-poison",
			payload = "INVALID NOT JSON".toByteArray()
		)

		every { inboxStore.findPendingPartitions() } returns listOf("part-1")
		every { inboxStore.claimBatch(any(), any(), "part-1") } returns listOf(record)

		poller.pollOnce()

		assertThat(consumer.received).isEmpty()
		verify(exactly = 1) { inboxStore.markDead(record.id) }
		verify(exactly = 1) { outboxStore.saveDlq(any()) }
		verify(exactly = 0) { inboxStore.markRetry(any(), any(), any()) }
	}

	@Test
	fun `should schedule retry on transient failure`() {
		val consumer = TestConsumer().apply { shouldFail = true }
		val method = consumer.javaClass.getMethod("onMessage", TestMessage::class.java)
		poller.registerListener("test.queue", consumer, method)

		val record = InboxRecord(
			id = UUID.randomUUID(),
			partitionKey = "part-1",
			type = "TestMessage",
			destination = "test.queue",
			businessKey = "biz-retry",
			payload = """{"message": "Retry me"}""".toByteArray(),
			attempts = 0
		)

		every { inboxStore.findPendingPartitions() } returns listOf("part-1")
		every { inboxStore.claimBatch(any(), any(), "part-1") } returns listOf(record)

		poller.pollOnce()

		verify(exactly = 1) { inboxStore.markRetry(record.id, 1, any()) }
		verify(exactly = 0) { inboxStore.markDead(any()) }
	}
}
