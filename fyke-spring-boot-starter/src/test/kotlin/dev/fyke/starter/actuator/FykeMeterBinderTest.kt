package dev.fyke.starter.actuator

import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.outbox.OutboxStore
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.support.StaticListableBeanFactory

class FykeMeterBinderTest {

	private val outboxStore = mockk<OutboxStore>(relaxed = true)
	private val inboxStore = mockk<InboxStore>(relaxed = true)

	@Test
	fun `bindTo should register gauges and reflect current counts`() {
		every { outboxStore.countPending() } returns 42L
		every { outboxStore.countDead() } returns 3L
		every { outboxStore.countUnreplayedDlq() } returns 7L
		every { inboxStore.countPending() } returns 15L
		every { inboxStore.countDead() } returns 1L

		val beanFactory = StaticListableBeanFactory()
		beanFactory.addBean("outboxStore", outboxStore)
		beanFactory.addBean("inboxStore", inboxStore)

		val binder = FykeMeterBinder(
			outboxStoreProvider = beanFactory.getBeanProvider(OutboxStore::class.java),
			inboxStoreProvider = beanFactory.getBeanProvider(InboxStore::class.java)
		)

		val registry = SimpleMeterRegistry()
		binder.bindTo(registry)

		val outboxBacklog = registry.find("fyke.outbox.backlog").gauge()
		val outboxDead = registry.find("fyke.outbox.dead").gauge()
		val dlqUnreplayed = registry.find("fyke.dlq.unreplayed").gauge()
		val inboxBacklog = registry.find("fyke.inbox.backlog").gauge()
		val inboxDead = registry.find("fyke.inbox.dead").gauge()

		assertThat(outboxBacklog).isNotNull
		assertThat(outboxBacklog!!.value()).isEqualTo(42.0)

		assertThat(outboxDead).isNotNull
		assertThat(outboxDead!!.value()).isEqualTo(3.0)

		assertThat(dlqUnreplayed).isNotNull
		assertThat(dlqUnreplayed!!.value()).isEqualTo(7.0)

		assertThat(inboxBacklog).isNotNull
		assertThat(inboxBacklog!!.value()).isEqualTo(15.0)

		assertThat(inboxDead).isNotNull
		assertThat(inboxDead!!.value()).isEqualTo(1.0)
	}
}
