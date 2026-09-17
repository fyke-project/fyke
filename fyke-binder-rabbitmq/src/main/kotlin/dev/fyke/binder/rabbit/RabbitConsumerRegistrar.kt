package dev.fyke.binder.rabbit

import dev.fyke.core.inbox.ConsumerPartitionResolver
import dev.fyke.core.inbox.DefaultConsumerPartitionResolver
import dev.fyke.core.inbox.FykeListener
import dev.fyke.core.inbox.InboxPollerEngine
import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.model.InboxRecord
import dev.fyke.core.model.InboxStatus
import org.slf4j.LoggerFactory
import org.springframework.amqp.core.AcknowledgeMode
import org.springframework.amqp.rabbit.connection.ConnectionFactory
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener
import org.springframework.beans.factory.SmartInitializingSingleton
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware
import org.springframework.context.SmartLifecycle
import java.lang.reflect.Method
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * Discovers @FykeListener annotated beans, registers them with the [InboxPollerEngine],
 * and sets up RabbitMQ listener containers that ingest messages into `fyke_inbox`.
 */
class RabbitConsumerRegistrar(
	private val connectionFactory: ConnectionFactory,
	private val inboxStore: InboxStore,
	private val inboxPollerEngine: InboxPollerEngine,
	private val defaultPartitionResolver: ConsumerPartitionResolver = DefaultConsumerPartitionResolver()
) : ApplicationContextAware, SmartInitializingSingleton, SmartLifecycle {

	private val log = LoggerFactory.getLogger(javaClass)
	private lateinit var applicationContext: ApplicationContext
	private val containers = ConcurrentHashMap<String, SimpleMessageListenerContainer>()
	private var running = false

	override fun setApplicationContext(applicationContext: ApplicationContext) {
		this.applicationContext = applicationContext
	}

	override fun afterSingletonsInstantiated() {
		log.debug("Fyke: Scanning beans for @FykeListener annotations")
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

		// Create container per destination queue if not already created
		containers.computeIfAbsent(destination) { queueName ->
			val resolver = if (annotation.partitionResolverBean.isNotBlank()) {
				try {
					applicationContext.getBean(annotation.partitionResolverBean, ConsumerPartitionResolver::class.java)
				} catch (e: Exception) {
					log.warn("Fyke: Could not find ConsumerPartitionResolver bean '{}', falling back to default",
						annotation.partitionResolverBean)
					defaultPartitionResolver
				}
			} else {
				defaultPartitionResolver
			}

			val effectiveConcurrency = if (annotation.ordering == dev.fyke.core.model.OrderingMode.STRICT_FIFO && annotation.concurrency > 1) {
				log.warn("Fyke: @FykeListener on destination '{}' has ordering=STRICT_FIFO and concurrency={}. " +
					"RabbitMQ queue ingestion concurrency has been forced to 1 to guarantee wire-order delivery into fyke_inbox. " +
					"Execution concurrency is handled concurrently across partitions by the inbox poller engine.",
					destination, annotation.concurrency)
				1
			} else {
				annotation.concurrency.coerceAtLeast(1)
			}

			SimpleMessageListenerContainer(connectionFactory).apply {
				setQueueNames(queueName)
				setAcknowledgeMode(AcknowledgeMode.MANUAL)
				setConcurrentConsumers(effectiveConcurrency)
				setMessageListener(ChannelAwareMessageListener { message, channel ->
					try {
						val properties = message.messageProperties
						val headers = properties.headers.mapValues { it.value?.toString() ?: "" }

						val businessKey = headers["x-fyke-business-key"]
							?: headers["business_key"]
							?: properties.correlationId
							?: properties.messageId
							?: UUID.randomUUID().toString()

						val type = headers["x-fyke-type"]
							?: headers["type"]
							?: properties.receivedRoutingKey
							?: "amqp.message"

						val messageId = properties.messageId ?: properties.correlationId

						log.debug(
							"Fyke: Received RabbitMQ message on queue '{}' (deliveryTag={}, messageId={})",
							destination,
							properties.deliveryTag,
							messageId
						)

						val partitionKey = resolver.resolve(
							headers = headers,
							payloadBytes = message.body,
							partitionKeyProperty = annotation.partitionKeyProperty.ifBlank { null }
						)
						log.trace(
							"Fyke: Resolved partition '{}' for RabbitMQ message (businessKey={}, type={})",
							partitionKey,
							businessKey,
							type
						)

						val inboxRecord = InboxRecord(
							id = UUID.randomUUID(),
							partitionKey = partitionKey,
							type = type,
							destination = destination,
							target = properties.receivedRoutingKey,
							businessKey = businessKey,
							messageId = messageId,
							status = InboxStatus.NEW,
							contentType = properties.contentType.ifBlank { "application/json" },
							payload = message.body,
							headers = headers,
							consumer = "${bean.javaClass.simpleName}.${method.name}",
							ordering = annotation.ordering
						)

						val saved = inboxStore.save(inboxRecord)
						channel?.basicAck(properties.deliveryTag, false)
						log.trace("Fyke: Acknowledged RabbitMQ deliveryTag={} (saved={})", properties.deliveryTag, saved)
						if (saved) {
							inboxPollerEngine.triggerPoll()
						}
					} catch (e: Exception) {
						log.error("Fyke: Failed to ingest RabbitMQ message into fyke_inbox", e)
						channel?.basicNack(message.messageProperties.deliveryTag, false, true)
					}
				})
			}
		}

		log.info("Fyke: Bound @FykeListener on queue '{}' to {}.{}()",
			destination, bean.javaClass.simpleName, method.name)
	}

	override fun start() {
		if (!running) {
			containers.values.forEach { container ->
				if (!container.isRunning) {
					container.start()
				}
			}
			running = true
			log.info("Fyke: RabbitConsumerRegistrar started {} listener containers", containers.size)
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
			log.info("Fyke: RabbitConsumerRegistrar stopped")
		}
	}

	override fun isRunning(): Boolean = running
}
