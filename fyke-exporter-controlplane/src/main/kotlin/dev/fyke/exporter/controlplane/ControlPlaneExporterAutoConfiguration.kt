package dev.fyke.exporter.controlplane

import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.core.telemetry.ClientSideSanitizer
import dev.fyke.exporter.controlplane.command.ControlPlaneCommandVerifier
import dev.fyke.exporter.controlplane.command.LocalCommandExecutor
import dev.fyke.exporter.controlplane.config.ControlPlaneProperties
import dev.fyke.exporter.controlplane.grpc.ControlPlaneGrpcClient
import dev.fyke.exporter.controlplane.heartbeat.ControlPlaneHeartbeatReporter
import dev.fyke.exporter.controlplane.telemetry.ControlPlaneEventListener
import dev.fyke.exporter.controlplane.telemetry.TelemetryRingBuffer
import dev.fyke.starter.FykeAutoConfiguration
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean

@AutoConfiguration(after = [FykeAutoConfiguration::class])
@ConditionalOnProperty(prefix = "fyke.control-plane", name = ["enabled"], havingValue = "true")
@EnableConfigurationProperties(ControlPlaneProperties::class)
class ControlPlaneExporterAutoConfiguration {

	private val log = LoggerFactory.getLogger(javaClass)

	@Bean
	@ConditionalOnMissingBean
	fun telemetryRingBuffer(
		properties: ControlPlaneProperties,
		sanitizer: ObjectProvider<ClientSideSanitizer>,
	): TelemetryRingBuffer {
		val resolvedSanitizer = sanitizer.getIfAvailable() ?: ClientSideSanitizer()
		return TelemetryRingBuffer(
			capacity = properties.buffer.capacity,
			sanitizer = resolvedSanitizer,
		)
	}

	@Bean
	@ConditionalOnMissingBean
	fun controlPlaneEventListener(ringBuffer: TelemetryRingBuffer): ControlPlaneEventListener {
		return ControlPlaneEventListener(ringBuffer)
	}

	@Bean
	@ConditionalOnMissingBean
	fun controlPlaneCommandVerifier(properties: ControlPlaneProperties): ControlPlaneCommandVerifier {
		return ControlPlaneCommandVerifier(
			enabled = properties.commandVerification.enabled,
			publicKeyBase64 = properties.commandVerification.publicKey,
		)
	}

	@Bean
	@ConditionalOnMissingBean
	fun localCommandExecutor(
		properties: ControlPlaneProperties,
		verifier: ControlPlaneCommandVerifier,
		outboxStore: ObjectProvider<OutboxStore>,
		inboxStore: ObjectProvider<InboxStore>,
	): LocalCommandExecutor {
		return LocalCommandExecutor(
			properties = properties,
			verifier = verifier,
			outboxStore = outboxStore.getIfAvailable(),
			inboxStore = inboxStore.getIfAvailable(),
		)
	}

	@Bean
	@ConditionalOnMissingBean
	fun controlPlaneHeartbeatReporter(
		outboxStore: ObjectProvider<OutboxStore>,
		inboxStore: ObjectProvider<InboxStore>,
	): ControlPlaneHeartbeatReporter {
		return ControlPlaneHeartbeatReporter(
			outboxStore = outboxStore.getIfAvailable(),
			inboxStore = inboxStore.getIfAvailable(),
		)
	}

	@Bean
	@ConditionalOnMissingBean
	fun controlPlaneGrpcClient(
		properties: ControlPlaneProperties,
		ringBuffer: TelemetryRingBuffer,
		heartbeatReporter: ControlPlaneHeartbeatReporter,
		commandExecutor: LocalCommandExecutor,
	): ControlPlaneGrpcClient {
		log.info("Fyke Control Plane Exporter enabled. Target endpoint: {}", properties.endpoint)
		return ControlPlaneGrpcClient(
			properties = properties,
			ringBuffer = ringBuffer,
			heartbeatReporter = heartbeatReporter,
			commandExecutor = commandExecutor,
		)
	}
}
