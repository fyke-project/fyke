package dev.fyke.binder.kafka

import dev.fyke.core.binder.PublishResult
import dev.fyke.core.inbox.ConsumerPartitionResolver
import dev.fyke.core.inbox.DefaultConsumerPartitionResolver
import dev.fyke.core.inbox.FykeListener
import dev.fyke.core.inbox.InboxPollerEngine
import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.model.InboxRecord
import dev.fyke.core.model.OrderingMode
import dev.fyke.core.model.OutboxRecord
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.context.ApplicationContext
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaBinderIntegrationTest {

	private val kafkaContainer = KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"))

	private lateinit var producerFactory: DefaultKafkaProducerFactory<String, ByteArray>
	private lateinit var kafkaTemplate: KafkaTemplate<String, ByteArray>
	private lateinit var binder: KafkaBinder

	@BeforeAll
	fun setUp() {
		kafkaContainer.start()

		val producerProps = mapOf<String, Any>(
			ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
			ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
			ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
			ProducerConfig.ACKS_CONFIG to "all"
		)
		producerFactory = DefaultKafkaProducerFactory(producerProps)
		kafkaTemplate = KafkaTemplate(producerFactory)
		binder = KafkaBinder(kafkaTemplate, confirmTimeoutMs = 10000L)
	}

	@AfterAll
	fun tearDown() {
		producerFactory.destroy()
		kafkaContainer.stop()
	}

	@Test
	fun `should publish event to real Kafka container and verify message with headers`() {
		assertThat(binder.isHealthy()).isTrue()

		val topic = "fyke.test.events"
		val recordId = UUID.randomUUID()
		val record = OutboxRecord(
			id = recordId,
			type = "OrderCreated",
			destination = topic,
			target = "order-456",
			businessKey = "order-456",
			idempotencyKey = "idem-456",
			payload = "{\"orderId\":\"order-456\",\"total\":99.50}".toByteArray(StandardCharsets.UTF_8),
			payloadHash = "hash456"
		)

		val result = binder.publish(record)
		assertThat(result).isEqualTo(PublishResult.Success)

		// Consume directly to verify
		val consumerProps = mapOf<String, Any>(
			ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
			ConsumerConfig.GROUP_ID_CONFIG to "test-verifier-group",
			ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
			ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
			ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java
		)

		KafkaConsumer<String, ByteArray>(consumerProps).use { consumer ->
			consumer.subscribe(listOf(topic))
			val records = consumer.poll(Duration.ofSeconds(10))
			assertThat(records.count()).isGreaterThanOrEqualTo(1)

			val matched = records.first { it.key() == "order-456" }
			assertThat(String(matched.value(), StandardCharsets.UTF_8))
				.isEqualTo("{\"orderId\":\"order-456\",\"total\":99.50}")

			val headers = matched.headers().associate { it.key() to String(it.value(), StandardCharsets.UTF_8) }
			assertThat(headers["x-fyke-idempotency-key"]).isEqualTo("idem-456")
			assertThat(headers["x-fyke-event-id"]).isEqualTo(recordId.toString())
			assertThat(headers["x-fyke-business-key"]).isEqualTo("order-456")
			assertThat(headers["x-fyke-type"]).isEqualTo("OrderCreated")
		}
	}

	@Test
	fun `should ingest message via KafkaConsumerRegistrar into inboxStore`() {
		val topic = "fyke.test.inbox"
		val consumerProps = mapOf<String, Any>(
			ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
			ConsumerConfig.GROUP_ID_CONFIG to "fyke-inbox-test-group",
			ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
			ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
			ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java
		)
		val consumerFactory = DefaultKafkaConsumerFactory<Any, Any>(consumerProps)

		val inboxStore = mockk<InboxStore>(relaxed = true)
		val inboxPoller = mockk<InboxPollerEngine>(relaxed = true)
		val savedInboxSlot = slot<InboxRecord>()
		every { inboxStore.save(capture(savedInboxSlot)) } returns true

		val listenerBean = SampleListener()
		val listenerMethod = SampleListener::class.java.getMethod("handleEvent", String::class.java)

		val appContext = mockk<ApplicationContext>(relaxed = true)
		every { appContext.beanDefinitionNames } returns arrayOf("sampleListener")
		every { appContext.getBean("sampleListener") } returns listenerBean

		val registrar = KafkaConsumerRegistrar(
			consumerFactory = consumerFactory,
			inboxStore = inboxStore,
			inboxPollerEngine = inboxPoller,
			defaultPartitionResolver = DefaultConsumerPartitionResolver()
		).apply {
			setApplicationContext(appContext)
			afterSingletonsInstantiated()
			start()
		}

		try {
			// Publish an event to the inbox topic
			val outboxRecord = OutboxRecord(
				id = UUID.randomUUID(),
				type = "UserRegistered",
				destination = topic,
				target = "user-789",
				businessKey = "user-789",
				idempotencyKey = "idem-789",
				payload = "{\"userId\":\"user-789\"}".toByteArray(StandardCharsets.UTF_8),
				payloadHash = "hash789"
			)
			val publishResult = binder.publish(outboxRecord)
			assertThat(publishResult).isEqualTo(PublishResult.Success)

			// Wait for registrar container to receive and save to inboxStore
			org.awaitility.Awaitility.await()
				.atMost(Duration.ofSeconds(15))
				.untilAsserted {
					verify(atLeast = 1) { inboxStore.save(any()) }
					verify(atLeast = 1) { inboxPoller.triggerPoll() }
				}

			val captured = savedInboxSlot.captured
			assertThat(captured.businessKey).isEqualTo("user-789")
			assertThat(captured.type).isEqualTo("UserRegistered")
			assertThat(captured.destination).isEqualTo(topic)
		} finally {
			registrar.stop()
		}
	}

	class SampleListener {
		@FykeListener(destination = "fyke.test.inbox", ordering = OrderingMode.STRICT_FIFO)
		fun handleEvent(event: String) {}
	}
}
