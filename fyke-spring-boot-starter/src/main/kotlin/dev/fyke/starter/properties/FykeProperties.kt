package dev.fyke.starter.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

/**
 * Configuration properties for Fyke transactional outbox, inbox, and reliability agent.
 */
@ConfigurationProperties(prefix = "fyke")
data class FykeProperties(
	/** Whether Fyke is enabled. Defaults to true. */
	var enabled: Boolean = true,
	/** Transactional outbox producer properties. */
	var outbox: OutboxProperties = OutboxProperties(),
	/** Transactional inbox consumer properties. */
	var inbox: InboxProperties = InboxProperties(),
	/** Dead letter queue retention properties. */
	var dlq: DlqProperties = DlqProperties(),
	/** Housekeeping cleaner schedule properties. */
	var retention: HousekeepingProperties = HousekeepingProperties(),
	/** Liquibase migration properties. */
	var liquibase: LiquibaseProperties = LiquibaseProperties(),
	/** Telemetry client-side payload sanitization properties. */
	var sanitization: SanitizationProperties = SanitizationProperties(),
	/** Selected broker binder ('rabbitmq', 'kafka', or empty for auto-detection). */
	var binder: String = "",
	/** RabbitMQ binder properties. */
	var rabbitmq: RabbitMqProperties = RabbitMqProperties(),
	/** Kafka binder properties. */
	var kafka: KafkaProperties = KafkaProperties()
) {
	/**
	 * Configurable retention settings for a specific queue or store.
	 */
	data class RetentionConfig(
		/** Whether retention purging is enabled for this store. */
		var enabled: Boolean = true,
		/** Time-to-live before records are eligible for deletion. */
		var ttl: Duration
	)

	/**
	 * Outbox polling, delivery, and retention configuration.
	 */
	data class OutboxProperties(
		/** Whether background outbox poller workers are active. */
		var enabled: Boolean = true,
		/** Maximum number of records claimed per partition batch. */
		var batchSize: Int = 50,
		/** Duration of the outbox lease lock before it expires and becomes reclaimable. */
		var leaseDuration: Duration = Duration.ofSeconds(30),
		/** Maximum delivery attempts before moving an outbox message to DLQ. */
		var maxAttempts: Int = 5,
		/** Interval for the periodic fallback timer poller. */
		var pollInterval: Duration = Duration.ofMillis(1000),
		/** Initial delay before the periodic timer starts. */
		var initialDelay: Duration = Duration.ofMillis(1000),
		/** Initial retry backoff duration for failed publications. */
		var initialBackoff: Duration = Duration.ofMillis(1000),
		/** Exponential backoff multiplier for retries. */
		var backoffMultiplier: Double = 1.5,
		/** Channel notification strategy (AUTO, PG_NOTIFY, TIMER). */
		var channel: PollerChannel = PollerChannel.AUTO,
		/** Number of concurrent worker threads processing partitions in the outbox poller. */
		var concurrency: Int = 1,
		/** Retention policy for PUBLISHED outbox records. */
		var retention: RetentionConfig = RetentionConfig(enabled = true, ttl = Duration.ofDays(7))
	)

	/**
	 * Transactional inbox polling, execution, and retention configuration.
	 */
	data class InboxProperties(
		/** Whether the transactional inbox poller is enabled. */
		var enabled: Boolean = true,
		/** Maximum number of records claimed per partition batch. */
		var batchSize: Int = 50,
		/** Duration of the inbox lease lock before it expires and becomes reclaimable. */
		var leaseDuration: Duration = Duration.ofSeconds(30),
		/** Maximum delivery attempts before moving an inbox message to DLQ. */
		var maxAttempts: Int = 5,
		/** Interval for the periodic inbox fallback timer. */
		var pollInterval: Duration = Duration.ofMillis(500),
		/** Initial delay before the periodic timer starts. */
		var initialDelay: Duration = Duration.ofMillis(1000),
		/** Initial retry backoff duration for failed consumer executions. */
		var initialBackoff: Duration = Duration.ofMillis(1000),
		/** Exponential backoff multiplier for retries. */
		var backoffMultiplier: Double = 1.5,
		/** Number of concurrent worker threads processing partitions in the inbox poller. */
		var concurrency: Int = 4,
		/** Channel notification strategy (AUTO, PG_NOTIFY, TIMER). */
		var channel: PollerChannel = PollerChannel.AUTO,
		/** Retention policy for COMPLETED inbox records (deduplication window). */
		var retention: RetentionConfig = RetentionConfig(enabled = true, ttl = Duration.ofDays(14))
	)

	/**
	 * Dead letter queue retention configuration.
	 */
	data class DlqProperties(
		/** Retention policy for REPLAYED dead-letter records. */
		var retention: RetentionConfig = RetentionConfig(enabled = true, ttl = Duration.ofDays(30))
	)

	/**
	 * Housekeeping cleaner background daemon schedule.
	 */
	data class HousekeepingProperties(
		/** Whether periodic retention purging is enabled. */
		var enabled: Boolean = true,
		/** How often the retention cleaner daemon runs. */
		var purgeInterval: Duration = Duration.ofHours(1),
		/** Maximum number of records deleted per purge query batch. */
		var batchSize: Int = 1000
	)

	/**
	 * Wakeup channel strategy used by the poller engines.
	 */
	enum class PollerChannel {
		/** Automatically selects PG_NOTIFY if connected to PostgreSQL, otherwise falls back to TIMER. */
		AUTO,
		/** Force PostgreSQL LISTEN/NOTIFY channel (requires PostgreSQL DataSource). */
		PG_NOTIFY,
		/** Force periodic timer polling interval. */
		TIMER
	}

	/**
	 * Liquibase database migration configuration.
	 */
	data class LiquibaseProperties(
		/** Whether Fyke should automatically run its embedded Liquibase changelog on startup. */
		var enabled: Boolean = true,
		/** Path to the Fyke master changelog XML. */
		var changeLog: String = "classpath:db/changelog/db.changelog-master.xml"
	)

	/**
	 * Telemetry sanitization configuration (R5).
	 */
	data class SanitizationProperties(
		/** If true, only metadata, headers, and payload hashes are recorded in telemetry spans. */
		var metadataOnly: Boolean = true,
		/** Whitelist of header keys allowed in telemetry. */
		var allowList: Set<String> = emptySet(),
		/** Blacklist of header keys stripped from telemetry. */
		var denyList: Set<String> = emptySet()
	)

	/**
	 * RabbitMQ binder configuration.
	 */
	data class RabbitMqProperties(
		/** Timeout waiting for RabbitMQ publisher ACK confirmation. */
		var confirmTimeout: Duration = Duration.ofSeconds(5)
	)

	/**
	 * Kafka binder configuration.
	 */
	data class KafkaProperties(
		/** Timeout waiting for Kafka publisher confirmation. */
		var confirmTimeout: Duration = Duration.ofSeconds(5)
	)
}
