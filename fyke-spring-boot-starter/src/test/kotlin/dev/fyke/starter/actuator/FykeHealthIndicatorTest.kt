package dev.fyke.starter.actuator

import dev.fyke.core.inbox.InboxPollerEngine
import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.outbox.OutboxPollerEngine
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.starter.properties.FykeProperties
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.support.StaticListableBeanFactory
import org.springframework.boot.health.contributor.Status

class FykeHealthIndicatorTest {

	private val outboxStore = mockk<OutboxStore>(relaxed = true)
	private val inboxStore = mockk<InboxStore>(relaxed = true)
	private val outboxPoller = mockk<OutboxPollerEngine>(relaxed = true)
	private val inboxPoller = mockk<InboxPollerEngine>(relaxed = true)
	private val properties = FykeProperties()

	private fun createIndicator(
		hasOutboxStore: Boolean = true,
		hasInboxStore: Boolean = true,
		hasOutboxPoller: Boolean = true,
		hasInboxPoller: Boolean = true
	): FykeHealthIndicator {
		val beanFactory = StaticListableBeanFactory()
		if (hasOutboxStore) beanFactory.addBean("outboxStore", outboxStore)
		if (hasInboxStore) beanFactory.addBean("inboxStore", inboxStore)
		if (hasOutboxPoller) beanFactory.addBean("outboxPollerEngine", outboxPoller)
		if (hasInboxPoller) beanFactory.addBean("inboxPollerEngine", inboxPoller)

		return FykeHealthIndicator(
			outboxStoreProvider = beanFactory.getBeanProvider(OutboxStore::class.java),
			inboxStoreProvider = beanFactory.getBeanProvider(InboxStore::class.java),
			outboxPollerEngineProvider = beanFactory.getBeanProvider(OutboxPollerEngine::class.java),
			inboxPollerEngineProvider = beanFactory.getBeanProvider(InboxPollerEngine::class.java),
			properties = properties
		)
	}

	@Test
	fun `health should be UP when engines are running and stores respond`() {
		every { outboxPoller.isRunning() } returns true
		every { inboxPoller.isRunning() } returns true
		every { outboxStore.countPending() } returns 12L
		every { outboxStore.countDead() } returns 2L
		every { outboxStore.countUnreplayedDlq() } returns 3L
		every { inboxStore.countPending() } returns 5L
		every { inboxStore.countDead() } returns 0L

		val indicator = createIndicator()
		val health = indicator.health()

		assertThat(health.status).isEqualTo(Status.UP)
		assertThat(health.details).containsKey("outbox")
		assertThat(health.details).containsKey("inbox")
		assertThat(health.details).containsKey("dlq")

		@Suppress("UNCHECKED_CAST")
		val outboxDetails = health.details["outbox"] as Map<String, Any>
		assertThat(outboxDetails["status"]).isEqualTo("RUNNING")
		assertThat(outboxDetails["pending"]).isEqualTo(12L)
		assertThat(outboxDetails["dead"]).isEqualTo(2L)

		@Suppress("UNCHECKED_CAST")
		val inboxDetails = health.details["inbox"] as Map<String, Any>
		assertThat(inboxDetails["status"]).isEqualTo("RUNNING")
		assertThat(inboxDetails["pending"]).isEqualTo(5L)
		assertThat(inboxDetails["dead"]).isEqualTo(0L)

		@Suppress("UNCHECKED_CAST")
		val dlqDetails = health.details["dlq"] as Map<String, Any>
		assertThat(dlqDetails["unreplayed"]).isEqualTo(3L)
	}

	@Test
	fun `health should remain UP even when outbox backlog is large during broker outage`() {
		every { outboxPoller.isRunning() } returns true
		every { inboxPoller.isRunning() } returns true
		every { outboxStore.countPending() } returns 10_000L
		every { outboxStore.countDead() } returns 0L
		every { outboxStore.countUnreplayedDlq() } returns 0L
		every { inboxStore.countPending() } returns 0L
		every { inboxStore.countDead() } returns 0L

		val indicator = createIndicator()
		val health = indicator.health()

		// Outbox accumulating backlogs during broker blips is by design; pod must not be restarted by k8s
		assertThat(health.status).isEqualTo(Status.UP)
		@Suppress("UNCHECKED_CAST")
		val outboxDetails = health.details["outbox"] as Map<String, Any>
		assertThat(outboxDetails["pending"]).isEqualTo(10_000L)
	}

	@Test
	fun `health should be DOWN if outbox poller engine is stopped`() {
		every { outboxPoller.isRunning() } returns false
		every { inboxPoller.isRunning() } returns true

		val indicator = createIndicator()
		val health = indicator.health()

		assertThat(health.status).isEqualTo(Status.DOWN)
		assertThat(health.details).containsKey("outboxFailure")
	}

	@Test
	fun `health should be DOWN if database query fails`() {
		every { outboxPoller.isRunning() } returns true
		every { inboxPoller.isRunning() } returns true
		every { outboxStore.countPending() } throws RuntimeException("Connection refused")

		val indicator = createIndicator()
		val health = indicator.health()

		assertThat(health.status).isEqualTo(Status.DOWN)
		assertThat(health.details).containsKey("error")
	}
}
