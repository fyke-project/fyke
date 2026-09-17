package dev.fyke.starter.actuator

import dev.fyke.core.binder.BrokerBinder
import dev.fyke.core.inbox.InboxListenerRegistration
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

class FykeEndpointTest {

	private val outboxStore = mockk<OutboxStore>(relaxed = true)
	private val inboxStore = mockk<InboxStore>(relaxed = true)
	private val outboxPoller = mockk<OutboxPollerEngine>(relaxed = true)
	private val inboxPoller = mockk<InboxPollerEngine>(relaxed = true)
	private val brokerBinder = mockk<BrokerBinder>(relaxed = true)
	private val properties = FykeProperties()

	private fun createEndpoint(): FykeEndpoint {
		val beanFactory = StaticListableBeanFactory()
		beanFactory.addBean("outboxStore", outboxStore)
		beanFactory.addBean("inboxStore", inboxStore)
		beanFactory.addBean("outboxPollerEngine", outboxPoller)
		beanFactory.addBean("inboxPollerEngine", inboxPoller)
		beanFactory.addBean("brokerBinder", brokerBinder)

		return FykeEndpoint(
			outboxStoreProvider = beanFactory.getBeanProvider(OutboxStore::class.java),
			inboxStoreProvider = beanFactory.getBeanProvider(InboxStore::class.java),
			outboxPollerEngineProvider = beanFactory.getBeanProvider(OutboxPollerEngine::class.java),
			inboxPollerEngineProvider = beanFactory.getBeanProvider(InboxPollerEngine::class.java),
			brokerBinderProvider = beanFactory.getBeanProvider(BrokerBinder::class.java),
			properties = properties
		)
	}

	@Test
	fun `fykeInfo should return comprehensive diagnostics snapshot`() {
		every { outboxPoller.isRunning() } returns true
		every { outboxPoller.batchSize } returns 50
		every { outboxPoller.concurrency } returns 1
		every { outboxStore.countPending() } returns 10L
		every { outboxStore.countDead() } returns 1L
		every { outboxStore.countUnreplayedDlq() } returns 4L

		class SampleService {
			fun onOrder(msg: String) {}
		}
		val sampleBean = SampleService()
		val sampleMethod = SampleService::class.java.getMethod("onOrder", String::class.java)

		every { inboxPoller.isRunning() } returns true
		every { inboxPoller.batchSize } returns 25
		every { inboxPoller.concurrency } returns 2
		every { inboxStore.countPending() } returns 3L
		every { inboxStore.countDead() } returns 0L
		every { inboxPoller.getRegisteredListeners() } returns mapOf(
			"orders.queue" to listOf(
				InboxListenerRegistration("orders.queue", sampleBean, sampleMethod, String::class.java)
			)
		)

		every { brokerBinder.name() } returns "rabbitmq"

		val endpoint = createEndpoint()
		val snapshot = endpoint.fykeInfo()

		assertThat(snapshot.outbox.status).isEqualTo("RUNNING")
		assertThat(snapshot.outbox.pending).isEqualTo(10L)
		assertThat(snapshot.outbox.dead).isEqualTo(1L)
		assertThat(snapshot.outbox.batchSize).isEqualTo(50)
		assertThat(snapshot.outbox.concurrency).isEqualTo(1)

		assertThat(snapshot.inbox.status).isEqualTo("RUNNING")
		assertThat(snapshot.inbox.pending).isEqualTo(3L)
		assertThat(snapshot.inbox.dead).isEqualTo(0L)
		assertThat(snapshot.inbox.registeredListeners).containsKey("orders.queue")
		assertThat(snapshot.inbox.registeredListeners["orders.queue"]).containsExactly("SampleService.onOrder(String)")

		assertThat(snapshot.dlq.unreplayed).isEqualTo(4L)
		assertThat(snapshot.binder.name).isEqualTo("rabbitmq")
		assertThat(snapshot.retention.enabled).isTrue()
	}
}
