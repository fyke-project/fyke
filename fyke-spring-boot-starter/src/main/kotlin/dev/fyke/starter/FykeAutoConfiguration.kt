package dev.fyke.starter

import com.fasterxml.jackson.databind.ObjectMapper
import dev.fyke.binder.rabbit.FykeRabbitDlqRecoverer
import dev.fyke.binder.rabbit.RabbitBinder
import dev.fyke.binder.rabbit.RabbitConsumerRegistrar
import dev.fyke.core.binder.BrokerBinder
import dev.fyke.core.inbox.ConsumerPartitionResolver
import dev.fyke.core.inbox.DefaultConsumerPartitionResolver
import dev.fyke.core.inbox.InboxPollerEngine
import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.inbox.JdbcInboxStore
import dev.fyke.core.partition.BusinessKeyPartitionResolver
import dev.fyke.core.partition.PartitionLocker
import dev.fyke.core.partition.PartitionResolver
import dev.fyke.core.partition.PostgresAdvisoryPartitionLocker
import dev.fyke.core.partition.SinglePartitionResolver
import dev.fyke.core.partition.SingleWorkerPartitionLocker
import dev.fyke.core.outbox.JdbcOutboxStore
import dev.fyke.core.outbox.JdbcOutboxWriter
import dev.fyke.core.outbox.OutboxPollerEngine
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.core.outbox.OutboxWriter
import dev.fyke.core.poller.NotificationSource
import dev.fyke.core.poller.PgNotifyChannel
import dev.fyke.core.poller.TimerChannel
import dev.fyke.core.retention.RetentionCleaner
import dev.fyke.core.serializer.FykePayloadSerializer
import dev.fyke.core.serializer.JacksonFykePayloadSerializer
import dev.fyke.core.telemetry.ClientSideSanitizer
import dev.fyke.core.telemetry.FykeTelemetry
import dev.fyke.starter.annotation.FykeEvent
import dev.fyke.starter.properties.FykeProperties
import io.opentelemetry.api.OpenTelemetry
import liquibase.integration.spring.SpringLiquibase
import org.slf4j.LoggerFactory
import org.springframework.amqp.rabbit.connection.ConnectionFactory
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.context.event.EventListener
import java.sql.Connection
import java.util.Optional
import javax.sql.DataSource

@AutoConfiguration(
	afterName = [
		"org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
		"org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration"
	]
)
@ConditionalOnClass(DataSource::class)
@ConditionalOnProperty(prefix = "fyke", name = ["enabled"], matchIfMissing = true)
@EnableConfigurationProperties(FykeProperties::class)
class FykeAutoConfiguration {

	private val log = LoggerFactory.getLogger(javaClass)

	@Bean
	@ConditionalOnProperty(prefix = "fyke.liquibase", name = ["enabled"], matchIfMissing = true)
	@ConditionalOnClass(SpringLiquibase::class)
	fun fykeLiquibase(dataSource: DataSource, properties: FykeProperties): SpringLiquibase {
		return SpringLiquibase().apply {
			this.dataSource = dataSource
			this.changeLog = properties.liquibase.changeLog
		}
	}

	@Bean
	@ConditionalOnMissingBean
	fun objectMapper(): ObjectMapper {
		return com.fasterxml.jackson.module.kotlin.jacksonObjectMapper().apply {
			registerModule(com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
			disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
		}
	}

	@Bean
	@ConditionalOnMissingBean
	fun fykePayloadSerializer(objectMapper: ObjectMapper): FykePayloadSerializer {
		return JacksonFykePayloadSerializer(objectMapper)
	}

	@Bean
	@ConditionalOnMissingBean
	fun partitionResolver(): PartitionResolver {
		return SinglePartitionResolver()
	}

	@Bean
	@ConditionalOnMissingBean
	fun clientSideSanitizer(properties: FykeProperties): ClientSideSanitizer {
		return ClientSideSanitizer(
			allowList = properties.sanitization.allowList,
			denyList = properties.sanitization.denyList,
			metadataOnly = properties.sanitization.metadataOnly
		)
	}

	@Bean
	@ConditionalOnMissingBean
	fun fykeTelemetry(
		sanitizer: ClientSideSanitizer,
		openTelemetry: Optional<OpenTelemetry>
	): FykeTelemetry {
		return FykeTelemetry(
			openTelemetry = openTelemetry.orElse(null),
			sanitizer = sanitizer
		)
	}

	@Bean
	@ConditionalOnMissingBean
	fun outboxStore(
		dataSource: DataSource,
		objectMapper: ObjectMapper
	): OutboxStore {
		return JdbcOutboxStore(dataSource, objectMapper)
	}

	@Bean
	@ConditionalOnMissingBean
	fun inboxStore(
		dataSource: DataSource,
		objectMapper: ObjectMapper
	): InboxStore {
		return JdbcInboxStore(dataSource, objectMapper)
	}

	@Bean
	@ConditionalOnMissingBean
	fun consumerPartitionResolver(objectMapper: ObjectMapper): ConsumerPartitionResolver {
		return DefaultConsumerPartitionResolver(objectMapper)
	}

	@Bean
	@ConditionalOnMissingBean
	fun partitionLocker(dataSource: DataSource): PartitionLocker {
		val isPostgres = isPostgres(dataSource)
		return if (isPostgres) {
			PostgresAdvisoryPartitionLocker()
		} else {
			log.info("Fyke: Non-PostgreSQL database detected. Falling back to in-JVM SingleWorkerPartitionLocker.")
			SingleWorkerPartitionLocker()
		}
	}

	@Bean
	@ConditionalOnClass(RabbitTemplate::class)
	@ConditionalOnBean(RabbitTemplate::class)
	@ConditionalOnMissingBean
	fun rabbitBinder(rabbitTemplate: RabbitTemplate, properties: FykeProperties): BrokerBinder {
		return RabbitBinder(
			rabbitTemplate = rabbitTemplate,
			confirmTimeoutMs = properties.rabbitmq.confirmTimeout.toMillis()
		)
	}

	@Bean
	@ConditionalOnClass(RabbitTemplate::class)
	@ConditionalOnBean(RabbitTemplate::class)
	@ConditionalOnMissingBean
	fun fykeRabbitDlqRecoverer(
		outboxStore: OutboxStore,
		telemetry: FykeTelemetry
	): FykeRabbitDlqRecoverer {
		return FykeRabbitDlqRecoverer(outboxStore, telemetry)
	}

	@Bean("outboxNotificationSources")
	fun outboxNotificationSources(
		dataSource: DataSource,
		properties: FykeProperties
	): List<NotificationSource> {
		val sources = mutableListOf<NotificationSource>()
		val isPostgres = isPostgres(dataSource)

		val channelMode = properties.outbox.channel
		if ((channelMode == FykeProperties.PollerChannel.AUTO || channelMode == FykeProperties.PollerChannel.PG_NOTIFY) && isPostgres) {
			sources.add(PgNotifyChannel(connectionSupplier = { dataSource.connection }, channelName = "fyke_outbox_events"))
		} else if (!isPostgres) {
			log.info("Fyke: Non-PostgreSQL database detected. Using TimerChannel fallback interval polling for outbox.")
		}

		// Always register TimerChannel for idle safety-net and fallback
		sources.add(
			TimerChannel(
				initialDelayMs = properties.outbox.initialDelay.toMillis(),
				pollIntervalMs = properties.outbox.pollInterval.toMillis(),
				name = "fyke-outbox-timer"
			)
		)

		return sources
	}

	@Bean("inboxNotificationSources")
	fun inboxNotificationSources(
		dataSource: DataSource,
		properties: FykeProperties
	): List<NotificationSource> {
		val sources = mutableListOf<NotificationSource>()
		val isPostgres = isPostgres(dataSource)

		val channelMode = properties.inbox.channel
		if ((channelMode == FykeProperties.PollerChannel.AUTO || channelMode == FykeProperties.PollerChannel.PG_NOTIFY) && isPostgres) {
			sources.add(PgNotifyChannel(connectionSupplier = { dataSource.connection }, channelName = "fyke_inbox_events"))
		} else if (!isPostgres) {
			log.info("Fyke: Non-PostgreSQL database detected. Using TimerChannel fallback interval polling for inbox.")
		}

		// Always register TimerChannel for idle safety-net and fallback
		sources.add(
			TimerChannel(
				initialDelayMs = properties.inbox.initialDelay.toMillis(),
				pollIntervalMs = properties.inbox.pollInterval.toMillis(),
				name = "fyke-inbox-timer"
			)
		)

		return sources
	}

	@Bean
	@ConditionalOnMissingBean
	fun outboxPollerEngine(
		outboxStore: OutboxStore,
		brokerBinderProvider: ObjectProvider<BrokerBinder>,
		partitionLocker: PartitionLocker,
		@Qualifier("outboxNotificationSources") outboxNotificationSources: List<NotificationSource>,
		telemetry: FykeTelemetry,
		dataSource: DataSource,
		properties: FykeProperties
	): OutboxPollerEngine {
		val brokerBinder = brokerBinderProvider.ifAvailable
			?: error("No BrokerBinder bean available. Ensure fyke-binder-rabbitmq (with Spring AMQP) is on the classpath or define a custom BrokerBinder bean.")
		return OutboxPollerEngine(
			outboxStore = outboxStore,
			brokerBinder = brokerBinder,
			partitionLocker = partitionLocker,
			notificationSources = outboxNotificationSources,
			telemetry = telemetry,
			dataSource = dataSource,
			batchSize = properties.outbox.batchSize,
			leaseDuration = properties.outbox.leaseDuration,
			maxAttempts = properties.outbox.maxAttempts,
			initialBackoffMs = properties.outbox.initialBackoff.toMillis(),
			backoffMultiplier = properties.outbox.backoffMultiplier,
			concurrency = properties.outbox.concurrency
		)
	}

	@Bean
	@ConditionalOnMissingBean
	fun outboxWriter(
		dataSource: DataSource,
		outboxStore: OutboxStore,
		serializer: FykePayloadSerializer,
		partitionResolver: PartitionResolver,
		telemetry: FykeTelemetry,
		pollerEngineProvider: ObjectProvider<OutboxPollerEngine>
	): OutboxWriter {
		return JdbcOutboxWriter(
			dataSource = dataSource,
			outboxStore = outboxStore,
			serializer = serializer,
			partitionResolver = partitionResolver,
			telemetry = telemetry,
			onCommitWakeup = { pollerEngineProvider.ifAvailable?.triggerPoll() }
		)
	}

	@Bean
	@ConditionalOnMissingBean
	fun inboxPollerEngine(
		inboxStore: InboxStore,
		outboxStore: OutboxStore,
		partitionLocker: PartitionLocker,
		@Qualifier("inboxNotificationSources") inboxNotificationSources: List<NotificationSource>,
		serializer: FykePayloadSerializer,
		telemetry: FykeTelemetry,
		dataSource: DataSource,
		properties: FykeProperties
	): InboxPollerEngine {
		return InboxPollerEngine(
			inboxStore = inboxStore,
			outboxStore = outboxStore,
			partitionLocker = partitionLocker,
			notificationSources = inboxNotificationSources,
			serializer = serializer,
			telemetry = telemetry,
			dataSource = dataSource,
			batchSize = properties.inbox.batchSize,
			leaseDuration = properties.inbox.leaseDuration,
			maxAttempts = properties.inbox.maxAttempts,
			initialBackoffMs = properties.inbox.initialBackoff.toMillis(),
			backoffMultiplier = properties.inbox.backoffMultiplier,
			concurrency = properties.inbox.concurrency
		)
	}

	@Bean
	@ConditionalOnClass(ConnectionFactory::class)
	@ConditionalOnBean(ConnectionFactory::class)
	@ConditionalOnMissingBean
	fun rabbitConsumerRegistrar(
		connectionFactory: ConnectionFactory,
		inboxStore: InboxStore,
		inboxPollerEngine: InboxPollerEngine,
		consumerPartitionResolver: ConsumerPartitionResolver
	): RabbitConsumerRegistrar {
		return RabbitConsumerRegistrar(
			connectionFactory = connectionFactory,
			inboxStore = inboxStore,
			inboxPollerEngine = inboxPollerEngine,
			defaultPartitionResolver = consumerPartitionResolver
		)
	}

	@Bean
	@ConditionalOnMissingBean
	@ConditionalOnProperty(prefix = "fyke.retention", name = ["enabled"], matchIfMissing = true)
	fun retentionCleaner(
		outboxStore: OutboxStore,
		inboxStore: InboxStore,
		properties: FykeProperties
	): RetentionCleaner {
		return RetentionCleaner(
			outboxStore = outboxStore,
			inboxStore = inboxStore,
			outboxRetentionEnabled = properties.outbox.retention.enabled,
			outboxTtl = properties.outbox.retention.ttl,
			inboxRetentionEnabled = properties.inbox.retention.enabled,
			inboxTtl = properties.inbox.retention.ttl,
			dlqRetentionEnabled = properties.dlq.retention.enabled,
			dlqTtl = properties.dlq.retention.ttl,
			purgeInterval = properties.retention.purgeInterval,
			batchSize = properties.retention.batchSize
		)
	}

	@Bean
	fun fykeLifecycle(
		outboxPollerEngine: OutboxPollerEngine,
		inboxPollerEngine: InboxPollerEngine,
		retentionCleaner: Optional<RetentionCleaner>,
		outboxWriter: OutboxWriter,
		outboxStore: OutboxStore,
		inboxStore: InboxStore,
		properties: FykeProperties
	): SmartLifecycle {
		return object : SmartLifecycle {
			private var running = false

			override fun start() {
				log.debug(
					"Fyke: SmartLifecycle starting (outboxEnabled={}, inboxEnabled={}, retentionEnabled={})",
					properties.outbox.enabled,
					properties.inbox.enabled,
					properties.retention.enabled
				)
				Fyke.initialize(outboxWriter, outboxPollerEngine, outboxStore, inboxStore, inboxPollerEngine)
				if (properties.outbox.enabled) {
					outboxPollerEngine.start()
				}
				if (properties.inbox.enabled) {
					inboxPollerEngine.start()
				}
				retentionCleaner.ifPresent { it.start() }
				running = true
			}

			override fun stop() {
				log.debug("Fyke: SmartLifecycle stopping components")
				retentionCleaner.ifPresent { it.stop() }
				inboxPollerEngine.stop()
				outboxPollerEngine.stop()
				running = false
			}

			override fun isRunning(): Boolean = running
		}
	}

	@Bean
	fun fykeEventDispatcher(outboxWriterProvider: ObjectProvider<OutboxWriter>): FykeEventDispatcher {
		return FykeEventDispatcher(outboxWriterProvider)
	}

	private fun isPostgres(dataSource: DataSource): Boolean {
		return try {
			dataSource.connection.use { conn ->
				conn.metaData.databaseProductName.equals("PostgreSQL", ignoreCase = true)
			}
		} catch (e: Exception) {
			false
		}
	}
}

class FykeEventDispatcher(
	private val outboxWriterProvider: ObjectProvider<OutboxWriter>
) {
	private val log = LoggerFactory.getLogger(javaClass)

	@EventListener
	fun handleFykeEvent(event: Any) {
		val annotation = event.javaClass.getAnnotation(FykeEvent::class.java) ?: return
		val outboxWriter = outboxWriterProvider.ifAvailable ?: return
		val type = annotation.type.ifBlank { event.javaClass.simpleName }
		val destination = annotation.destination
		val target = annotation.target.ifBlank { null }

		// Extract business key property
		val businessKeyProp = annotation.businessKeyProperty
		val businessKeyValue = try {
			val field = event.javaClass.declaredFields.find { it.name == businessKeyProp }?.apply { isAccessible = true }
			field?.get(event)?.toString() ?: event.toString()
		} catch (_: Exception) {
			event.toString()
		}

		log.debug("Fyke: Intercepted @FykeEvent on {} for destination '{}'", event.javaClass.simpleName, destination)
		log.trace("Fyke: @FykeEvent details: type={}, target={}, businessKey={}", type, target, businessKeyValue)

		outboxWriter.write(
			dev.fyke.core.model.OutboxEvent(
				type = type,
				destination = destination,
				target = target,
				businessKey = businessKeyValue,
				payload = event
			)
		)
	}
}
