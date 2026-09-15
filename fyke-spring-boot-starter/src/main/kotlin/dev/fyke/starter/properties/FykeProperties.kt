package dev.fyke.starter.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

/**
 * Configuration properties for Fyke transactional outbox and reliability agent.
 */
@ConfigurationProperties(prefix = "fyke")
data class FykeProperties(
	/** Whether Fyke is enabled. Defaults to true. */
	var enabled: Boolean = true,
	/** Liquibase migration properties. */
	var liquibase: LiquibaseProperties = LiquibaseProperties(),
	/** Background poller and dispatch engine properties. */
	var poller: PollerProperties = PollerProperties(),
	/** Transactional inbox polling and execution engine properties. */
	var inbox: InboxProperties = InboxProperties(),
	/** Housekeeping and retention cleaner properties. */
	var retention: RetentionProperties = RetentionProperties(),
	/** Telemetry client-side payload sanitization properties. */
	var sanitization: SanitizationProperties = SanitizationProperties(),
	/** RabbitMQ binder properties. */
	var rabbitmq: RabbitMqProperties = RabbitMqProperties()
) {
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
	 * Outbox polling and delivery engine configuration.
	 */
	data class PollerProperties(
		/** Whether background poller workers are active. */
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
		var concurrency: Int = 1
	)

	/**
	 * Wakeup channel strategy used by the poller engine.
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
	 * Retention and housekeeping cleaner configuration (R10).
	 */
	data class RetentionProperties(
		/** Whether periodic retention purging of published and dead-lettered events is enabled. */
		var enabled: Boolean = true,
		/** Retention time-to-live for successfully PUBLISHED outbox records before deletion. */
		var outboxTtl: Duration = Duration.ofDays(7),
		/** Retention time-to-live for COMPLETED inbox records before deletion. */
		var inboxTtl: Duration = Duration.ofDays(7),
		/** Retention time-to-live for REPLAYED dead-letter records before deletion. */
		var dlqTtl: Duration = Duration.ofDays(30),
		/** How often the retention cleaner runs. */
		var purgeInterval: Duration = Duration.ofHours(1),
		/** Maximum number of records deleted per purge query batch. */
		var batchSize: Int = 1000
	)

	/**
	 * Transactional inbox configuration.
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
		var channel: PollerChannel = PollerChannel.AUTO
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
}
