package dev.fyke.starter

import dev.fyke.core.inbox.InboxPollerEngine
import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.model.FykeRecordSummary
import dev.fyke.core.model.OutboxEvent
import dev.fyke.core.model.OutboxRecord
import dev.fyke.core.outbox.OutboxPollerEngine
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.core.outbox.OutboxWriter
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID

class FykeFacadeTest {

	private val writer = mockk<OutboxWriter>()
	private val poller = mockk<OutboxPollerEngine>()
	private val store = mockk<OutboxStore>()

	@BeforeEach
	fun setUp() {
		Fyke.initialize(writer, poller, store)
	}

	@Test
	fun `Fyke send should delegate to OutboxWriter`() {
		val event = OutboxEvent(type = "OrderCreated", destination = "orders", businessKey = "ord-1", payload = "{}")
		val expectedRecord = OutboxRecord(
			id = UUID.randomUUID(),
			type = "OrderCreated",
			destination = "orders",
			businessKey = "ord-1",
			idempotencyKey = "idem-1",
			payload = "{}".toByteArray(),
			payloadHash = "hash"
		)

		every { writer.write(event) } returns expectedRecord

		val result = Fyke.send(event)

		assertThat(result).isEqualTo(expectedRecord)
		verify(exactly = 1) { writer.write(event) }
	}

	@Test
	fun `Fyke replay should delegate to OutboxPollerEngine`() {
		val id = UUID.randomUUID()
		every { poller.replay(id) } returns true

		val success = Fyke.replay(id)

		assertThat(success).isTrue()
		verify(exactly = 1) { poller.replay(id) }
	}

	@Test
	fun `Fyke replayOutbox should delegate to OutboxPollerEngine`() {
		val id = UUID.randomUUID()
		every { poller.replay(id) } returns true

		val success = Fyke.replayOutbox(id)

		assertThat(success).isTrue()
		verify(exactly = 1) { poller.replay(id) }
	}

	@Test
	fun `Fyke searchByBusinessKey should delegate to OutboxStore`() {
		val summaries = listOf(
			FykeRecordSummary(
				id = UUID.randomUUID(),
				source = "OUTBOX",
				type = "OrderCreated",
				businessKey = "ord-1",
				status = "PUBLISHED",
				destination = "orders",
				target = null,
				timestamp = Instant.now()
			)
		)
		every { store.searchByBusinessKey("ord-1") } returns summaries

		val results = Fyke.searchByBusinessKey("ord-1")

		assertThat(results).isEqualTo(summaries)
		verify(exactly = 1) { store.searchByBusinessKey("ord-1") }
	}

	@Test
	fun `Fyke retryInbox should delegate to InboxStore and trigger poller`() {
		val inboxStore = mockk<InboxStore>()
		val inboxPoller = mockk<InboxPollerEngine>(relaxed = true)
		Fyke.initialize(writer, poller, store, inboxStore, inboxPoller)

		val id = UUID.randomUUID()
		every { inboxStore.retryNow(id) } returns true

		val success = Fyke.retryInbox(id)

		assertThat(success).isTrue()
		verify(exactly = 1) { inboxStore.retryNow(id) }
		verify(exactly = 1) { inboxPoller.triggerPoll() }
	}
}
