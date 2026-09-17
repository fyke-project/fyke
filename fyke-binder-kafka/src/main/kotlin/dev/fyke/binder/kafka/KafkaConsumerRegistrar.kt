package dev.fyke.binder.kafka

import dev.fyke.core.inbox.ConsumerPartitionResolver
import dev.fyke.core.inbox.DefaultConsumerPartitionResolver
import dev.fyke.core.inbox.FykeListener
import dev.fyke.core.inbox.InboxPollerEngine
import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.model.InboxRecord
import dev.fyke.core.model.InboxStatus
import dev.fyke.core.model.OrderingMode
import dev.fyke.core.telemetry.FykeTelemetry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.SmartInitializingSingleton
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware
import org.springframework.context.SmartLifecycle
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.AcknowledgingMessageListener
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import java.lang.reflect.Method
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * Discovers @FykeListener annotated beans, registers them with the [InboxPollerEngine],
 * and sets up Kafka listener containers that ingest messages into `fyke_inbox`.
 */
class KafkaConsumerRegistrar(
	private val consumerFactory: ConsumerFactory<Any, Any>,
	private val inboxStore: InboxStore,
	private val inboxPollerEngine: InboxPollerEngine,
	private val defaultPartitionResolver: ConsumerPartitionResolver = DefaultConsumerPartitionResolver(),
	private val telemetry: FykeTelemetry? = null
) : ApplicationContextAware, SmartInitializingSingleton, SmartLifecycle {

	private val log = LoggerFactory.getLogger(javaClass)
	private lateinit var applicationContext: ApplicationContext
	private val containers = ConcurrentHashMap<String, ConcurrentMessageListenerContainer<Any, Any>>()
	private var running = false

	override fun setApplicationContext(applicationContext: ApplicationContext) {
		this.applicationContext = applicationContext
	}

	override fun afterSingletonsInstantiated() {
		log.debug("Fyke: Scanning beans for @FykeListener annotations (Kafka)")
		val beanNames = applicationContext.beanDefinitionNames
		for (beanName in beanNames) {
			val bean = try {
				applicationContext.getBean(beanName)
			} catch (_: Exception) {
				continue
			}

			val targetClass = bean.javaClass
			for (method in targetClass.methods) {
				val annotation = method.getAnnotation(FykeListener::class.java) ?: continue
				registerFykeListener(bean, method, annotation)
			}
		}
	}

	private fun registerFykeListener(bean: Any, method: Method, annotation: FykeListener) {
		val env = try {
			applicationContext.environment
		} catch (_: Exception) {
			null
		}
		val destination = env?.resolvePlaceholders(annotation.destination)?.ifBlank { null } ?: annotation.destination
		inboxPollerEngine.registerListener(destination, bean, method)

		containers.computeIfAbsent(destination) { topic ->
			val resolver = if (annotation.partitionResolverBean.isNotBlank()) {
				try {
					applicationContext.getBean(annotation.partitionResolverBean, ConsumerPartitionResolver::class.java)
				} catch (e: Exception) {
					log.warn(
						"Fyke: Could not find ConsumerPartitionResolver bean '{}', falling back to default",
						annotation.partitionResolverBean
					)
					defaultPartitionResolver
				}
			} else {
				defaultPartitionResolver
			}

			val rawGroupId = annotation.consumerGroup.ifBlank { "fyke-consumer-$topic" }
			val groupId = env?.resolvePlaceholders(rawGroupId)?.ifBlank { null } ?: rawGroupId
			val containerProps = ContainerProperties(topic).apply {
				setGroupId(groupId)
				setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE)
				setMessageListener(AcknowledgingMessageListener<Any, Any> { record, acknowledgment ->
					try {
						val headers = mutableMapOf<String, String>()
						record.headers().forEach { header ->
							headers[header.key()] = String(header.value(), StandardCharsets.UTF_8)
						}

						val businessKey = headers["x-fyke-business-key"]
							?: headers["business_key"]
							?: record.key()?.toString()
							?: UUID.randomUUID().toString()

						val type = headers["x-fyke-type"]
							?: headers["type"]
							?: topic

						val messageId = "${record.topic()}-${record.partition()}-${record.offset()}"

						val rawPayload = when (val value = record.value()) {
							is ByteArray -> value
							is String -> value.toByteArray(StandardCharsets.UTF_8)
							else -> value?.toString()?.toByteArray(StandardCharsets.UTF_8) ?: ByteArray(0)
						}

						log.debug(
							"Fyke: Received Kafka record on topic '{}' (partition={}, offset={}, messageId={})",
							topic,
							record.partition(),
							record.offset(),
							messageId
						)

						val partitionKey = resolver.resolve(
							headers = headers,
							payloadBytes = rawPayload,
							partitionKeyProperty = annotation.partitionKeyProperty.ifBlank { null }
						)
						log.trace(
							"Fyke: Resolved partition '{}' for Kafka record (businessKey={}, type={})",
							partitionKey,
							businessKey,
							type
						)

						val inboxRecord = InboxRecord(
							id = UUID.randomUUID(),
							partitionKey = partitionKey,
							type = type,
							destination = topic,
							target = "${record.partition()}:${record.offset()}",
							businessKey = businessKey,
							messageId = messageId,
							status = InboxStatus.NEW,
							contentType = headers["x-fyke-content-type"] ?: "application/json",
							payload = rawPayload,
							headers = headers,
							consumer = "${bean.javaClass.simpleName}.${method.name}",
							ordering = annotation.ordering
						)

						val saved = inboxStore.save(inboxRecord)
						acknowledgment?.acknowledge()
						log.trace(
							"Fyke: Acknowledged Kafka offset {}:{} (saved={})",
							record.partition(),
							record.offset(),
							saved
						)
						if (saved) {
							telemetry?.notifyInboxReceived(inboxRecord)
							inboxPollerEngine.triggerPoll()
						}
					} catch (e: Exception) {
						log.error("Fyke: Failed to ingest Kafka record into fyke_inbox", e)
					}
				})
			}

			val concurrency = if (annotation.ordering == OrderingMode.STRICT_FIFO && annotation.concurrency > 1) {
				log.warn(
					"Fyke: @FykeListener on destination '{}' has ordering=STRICT_FIFO and concurrency={}. " +
						"Kafka container concurrency has been set to 1 to preserve partition sequencing into fyke_inbox. " +
						"Processing concurrency is handled per partition by the inbox poller engine.",
					destination,
					annotation.concurrency
				)
				1
			} else {
				annotation.concurrency.coerceAtLeast(1)
			}

			ConcurrentMessageListenerContainer(consumerFactory, containerProps).apply {
				setConcurrency(concurrency)
			}
		}

		log.info(
			"Fyke: Bound @FykeListener on Kafka topic '{}' to {}.{}()",
			destination,
			bean.javaClass.simpleName,
			method.name
		)
	}

	override fun start() {
		if (!running) {
			containers.values.forEach { container ->
				if (!container.isRunning) {
					container.start()
				}
			}
			running = true
			log.info("Fyke: KafkaConsumerRegistrar started {} listener containers", containers.size)
		}
	}

	override fun stop() {
		if (running) {
			containers.values.forEach { container ->
				if (container.isRunning) {
					container.stop()
				}
			}
			running = false
			log.info("Fyke: KafkaConsumerRegistrar stopped")
		}
	}

	override fun isRunning(): Boolean = running
}
