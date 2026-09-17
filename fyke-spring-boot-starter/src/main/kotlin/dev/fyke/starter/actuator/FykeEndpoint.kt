package dev.fyke.starter.actuator

import dev.fyke.core.binder.BrokerBinder
import dev.fyke.core.inbox.InboxPollerEngine
import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.outbox.OutboxPollerEngine
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.starter.properties.FykeProperties
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.actuate.endpoint.annotation.Endpoint
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation

/**
 * Actuator management endpoint exposing comprehensive operational diagnostics for Fyke.
 *
 * Available at `/actuator/fyke`.
 */
@Endpoint(id = "fyke")
class FykeEndpoint(
	private val outboxStoreProvider: ObjectProvider<OutboxStore>,
	private val inboxStoreProvider: ObjectProvider<InboxStore>,
	private val outboxPollerEngineProvider: ObjectProvider<OutboxPollerEngine>,
	private val inboxPollerEngineProvider: ObjectProvider<InboxPollerEngine>,
	private val brokerBinderProvider: ObjectProvider<BrokerBinder>,
	private val properties: FykeProperties
) {

	@ReadOperation
	fun fykeInfo(): FykeDiagnosticsSnapshot {
		val outboxStore = outboxStoreProvider.ifAvailable
		val inboxStore = inboxStoreProvider.ifAvailable
		val outboxPoller = outboxPollerEngineProvider.ifAvailable
		val inboxPoller = inboxPollerEngineProvider.ifAvailable
		val binder = brokerBinderProvider.ifAvailable

		val outboxDiag = OutboxDiagnostics(
			status = if (outboxPoller?.isRunning() == true) "RUNNING" else "STOPPED",
			pending = outboxStore?.countPending() ?: 0L,
			dead = outboxStore?.countDead() ?: 0L,
			batchSize = outboxPoller?.batchSize ?: properties.outbox.batchSize,
			concurrency = outboxPoller?.concurrency ?: properties.outbox.concurrency,
			channel = properties.outbox.channel.name,
			maxAttempts = properties.outbox.maxAttempts
		)

		val listeners = inboxPoller?.getRegisteredListeners()?.mapValues { (_, regList) ->
			regList.map { "${it.bean.javaClass.simpleName}.${it.method.name}(${it.targetType.simpleName})" }
		} ?: emptyMap()

		val inboxDiag = InboxDiagnostics(
			status = if (inboxPoller?.isRunning() == true) "RUNNING" else "STOPPED",
			pending = inboxStore?.countPending() ?: 0L,
			dead = inboxStore?.countDead() ?: 0L,
			batchSize = inboxPoller?.batchSize ?: properties.inbox.batchSize,
			concurrency = inboxPoller?.concurrency ?: properties.inbox.concurrency,
			channel = properties.inbox.channel.name,
			maxAttempts = properties.inbox.maxAttempts,
			registeredListeners = listeners
		)

		val dlqDiag = DlqDiagnostics(
			unreplayed = outboxStore?.countUnreplayedDlq() ?: 0L
		)

		val binderDiag = BinderDiagnostics(
			name = binder?.name() ?: if (properties.binder.isNotBlank()) properties.binder else "NONE",
			confirmTimeoutMs = properties.rabbitmq.confirmTimeout.toMillis().coerceAtLeast(properties.kafka.confirmTimeout.toMillis())
		)

		val retentionDiag = RetentionDiagnostics(
			enabled = properties.retention.enabled,
			purgeIntervalSeconds = properties.retention.purgeInterval.seconds,
			outboxTtlSeconds = properties.outbox.retention.ttl.seconds,
			inboxTtlSeconds = properties.inbox.retention.ttl.seconds,
			dlqTtlSeconds = properties.dlq.retention.ttl.seconds
		)

		return FykeDiagnosticsSnapshot(
			outbox = outboxDiag,
			inbox = inboxDiag,
			dlq = dlqDiag,
			binder = binderDiag,
			retention = retentionDiag
		)
	}

	data class FykeDiagnosticsSnapshot(
		val outbox: OutboxDiagnostics,
		val inbox: InboxDiagnostics,
		val dlq: DlqDiagnostics,
		val binder: BinderDiagnostics,
		val retention: RetentionDiagnostics
	)

	data class OutboxDiagnostics(
		val status: String,
		val pending: Long,
		val dead: Long,
		val batchSize: Int,
		val concurrency: Int,
		val channel: String,
		val maxAttempts: Int
	)

	data class InboxDiagnostics(
		val status: String,
		val pending: Long,
		val dead: Long,
		val batchSize: Int,
		val concurrency: Int,
		val channel: String,
		val maxAttempts: Int,
		val registeredListeners: Map<String, List<String>>
	)

	data class DlqDiagnostics(
		val unreplayed: Long
	)

	data class BinderDiagnostics(
		val name: String,
		val confirmTimeoutMs: Long
	)

	data class RetentionDiagnostics(
		val enabled: Boolean,
		val purgeIntervalSeconds: Long,
		val outboxTtlSeconds: Long,
		val inboxTtlSeconds: Long,
		val dlqTtlSeconds: Long
	)
}
