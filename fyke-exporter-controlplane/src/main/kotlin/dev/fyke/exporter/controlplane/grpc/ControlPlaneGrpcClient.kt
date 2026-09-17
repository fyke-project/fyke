package dev.fyke.exporter.controlplane.grpc

import com.google.protobuf.Timestamp
import dev.fyke.controlplane.v1.AgentCapability
import dev.fyke.controlplane.v1.AgentEnvelope
import dev.fyke.controlplane.v1.AppIdentity
import dev.fyke.controlplane.v1.ControlPlaneEnvelope
import dev.fyke.controlplane.v1.FykeControlPlaneServiceGrpc
import dev.fyke.controlplane.v1.HandshakeRequest
import dev.fyke.controlplane.v1.TelemetryBatchReport
import dev.fyke.exporter.controlplane.command.LocalCommandExecutor
import dev.fyke.exporter.controlplane.config.ControlPlaneProperties
import dev.fyke.exporter.controlplane.heartbeat.ControlPlaneHeartbeatReporter
import dev.fyke.exporter.controlplane.telemetry.TelemetryRingBuffer
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Metadata
import io.grpc.stub.MetadataUtils
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class ControlPlaneGrpcClient(
	private val properties: ControlPlaneProperties,
	private val ringBuffer: TelemetryRingBuffer,
	private val heartbeatReporter: ControlPlaneHeartbeatReporter,
	private val commandExecutor: LocalCommandExecutor,
	managedChannel: ManagedChannel? = null
) : SmartLifecycle {

	private val log = LoggerFactory.getLogger(javaClass)

	private val running = AtomicBoolean(false)
	private val connected = AtomicBoolean(false)

	private val providedChannel = managedChannel != null
	private val channel: ManagedChannel = managedChannel ?: buildChannel(properties)

	private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor { r ->
		Thread(r, "fyke-controlplane-exporter").apply { isDaemon = true }
	}

	@Volatile
	private var requestStream: StreamObserver<AgentEnvelope>? = null

	@Volatile
	private var heartbeatTask: ScheduledFuture<*>? = null

	@Volatile
	private var telemetryTask: ScheduledFuture<*>? = null

	@Volatile
	private var heartbeatIntervalSeconds = properties.heartbeatIntervalSeconds

	@Volatile
	private var flushIntervalMs = properties.buffer.flushIntervalMs

	@Volatile
	private var maxBatchSize = properties.buffer.batchSize

	@Volatile
	private var sessionId: String? = null

	override fun isRunning(): Boolean = running.get()

	override fun getPhase(): Int = Integer.MAX_VALUE - 100

	override fun start() {
		if (running.compareAndSet(false, true)) {
			log.info("Starting Fyke Control Plane Exporter (connecting to {})", properties.endpoint)
			scheduleConnect(0)
		}
	}

	override fun stop() {
		if (running.compareAndSet(true, false)) {
			log.info("Stopping Fyke Control Plane Exporter")
			heartbeatTask?.cancel(true)
			telemetryTask?.cancel(true)

			try {
				requestStream?.onCompleted()
			} catch (e: Exception) {
				// Ignore shutdown error
			}
			requestStream = null

			if (!providedChannel) {
				channel.shutdownNow()
			}
			executor.shutdownNow()
		}
	}

	private fun scheduleConnect(delaySeconds: Long) {
		if (!running.get()) return
		executor.schedule({
			try {
				connect()
			} catch (e: Exception) {
				log.warn("Failed to initiate connection to control plane: {}", e.message)
				scheduleReconnect()
			}
		}, delaySeconds, TimeUnit.SECONDS)
	}

	private fun scheduleReconnect() {
		if (!running.get()) return
		connected.set(false)
		heartbeatTask?.cancel(true)
		telemetryTask?.cancel(true)
		requestStream = null

		val backoffSeconds = 5L
		log.debug("Scheduling reconnection to control plane in {} seconds", backoffSeconds)
		scheduleConnect(backoffSeconds)
	}

	@Synchronized
	private fun connect() {
		if (!running.get()) return

		var stub = FykeControlPlaneServiceGrpc.newStub(channel)
		if (properties.apiKey.isNotBlank()) {
			val headers = Metadata()
			val key = Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER)
			headers.put(key, "Bearer ${properties.apiKey}")
			stub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
		}

		val responseObserver = object : StreamObserver<ControlPlaneEnvelope> {
			override fun onNext(envelope: ControlPlaneEnvelope) {
				handleIncomingEnvelope(envelope)
			}

			override fun onError(t: Throwable) {
				log.warn("Control plane gRPC stream error: {}", t.message)
				scheduleReconnect()
			}

			override fun onCompleted() {
				log.info("Control plane gRPC stream closed by server")
				scheduleReconnect()
			}
		}

		val stream = stub.connectStream(responseObserver)
		this.requestStream = stream

		// Send initial Handshake
		sendHandshake(stream)
	}

	private fun sendHandshake(stream: StreamObserver<AgentEnvelope>) {
		val now = Instant.now()
		val identity = AppIdentity.newBuilder()
			.setTenantId(properties.tenantId)
			.setAppName(properties.appName)
			.setEnvironment(properties.environment)
			.setInstanceId(properties.instanceId)
			.setAgentVersion("0.1.0-SNAPSHOT")
			.setSpringBootVersion("4.1.1")
			.setJvmVersion(System.getProperty("java.version"))
			.addActiveBinders("rabbitmq")
			.build()

		val handshake = HandshakeRequest.newBuilder()
			.setIdentity(identity)
			.addCapabilities(AgentCapability.AGENT_CAPABILITY_OUTBOX_CAPTURE)
			.addCapabilities(AgentCapability.AGENT_CAPABILITY_INBOX_CAPTURE)
			.addCapabilities(AgentCapability.AGENT_CAPABILITY_DLQ_CAPTURE)
			.addCapabilities(AgentCapability.AGENT_CAPABILITY_ACTIONS_IN_JVM)
			.apply {
				if (properties.allowPayloadFetch) {
					addCapabilities(AgentCapability.AGENT_CAPABILITY_PAYLOAD_FETCH)
				}
			}
			.setStartupTime(
				Timestamp.newBuilder()
					.setSeconds(now.epochSecond)
					.setNanos(now.nano)
					.build()
			)
			.build()

		val envelope = AgentEnvelope.newBuilder()
			.setTraceId(UUID.randomUUID().toString())
			.setHandshake(handshake)
			.build()

		stream.onNext(envelope)
		log.debug("Sent HandshakeRequest to control plane (tenantId={}, appName={})", properties.tenantId, properties.appName)
	}

	private fun handleIncomingEnvelope(envelope: ControlPlaneEnvelope) {
		when (envelope.payloadCase) {
			ControlPlaneEnvelope.PayloadCase.HANDSHAKE_ACK -> {
				val ack = envelope.handshakeAck
				if (ack.accepted) {
					sessionId = ack.sessionId
					connected.set(true)
					log.info("Handshake accepted by control plane. Session ID: {}", ack.sessionId)

					if (ack.heartbeatIntervalSeconds > 0) {
						heartbeatIntervalSeconds = ack.heartbeatIntervalSeconds
					}
					if (ack.telemetryFlushIntervalMs > 0) {
						flushIntervalMs = ack.telemetryFlushIntervalMs.toLong()
					}
					if (ack.telemetryMaxBatchSize > 0) {
						maxBatchSize = ack.telemetryMaxBatchSize
					}

					startScheduledTasks()
				} else {
					log.error("Handshake rejected by control plane: {}", ack.rejectionReason)
					scheduleReconnect()
				}
			}

			ControlPlaneEnvelope.PayloadCase.HEARTBEAT_ACK -> {
				log.trace("Received HeartbeatAck from control plane")
			}

			ControlPlaneEnvelope.PayloadCase.COMMAND -> {
				val cmd = envelope.command
				log.debug("Received remote command from control plane: commandId={}, case={}", cmd.commandId, cmd.commandCase)
				val result = commandExecutor.execute(cmd)

				val responseEnvelope = AgentEnvelope.newBuilder()
					.setTraceId(UUID.randomUUID().toString())
					.setCommandResult(result)
					.build()

				sendEnvelope(responseEnvelope)
			}

			else -> {
				log.warn("Received unexpected envelope payload case: {}", envelope.payloadCase)
			}
		}
	}

	private fun startScheduledTasks() {
		heartbeatTask?.cancel(true)
		telemetryTask?.cancel(true)

		heartbeatTask = executor.scheduleWithFixedDelay(
			{ sendHeartbeat() },
			heartbeatIntervalSeconds.toLong(),
			heartbeatIntervalSeconds.toLong(),
			TimeUnit.SECONDS
		)

		telemetryTask = executor.scheduleWithFixedDelay(
			{ flushTelemetry() },
			flushIntervalMs,
			flushIntervalMs,
			TimeUnit.MILLISECONDS
		)
	}

	fun sendHeartbeat() {
		if (!connected.get()) return
		try {
			val report = heartbeatReporter.buildHeartbeatReport()
			val envelope = AgentEnvelope.newBuilder()
				.setTraceId(UUID.randomUUID().toString())
				.setHeartbeat(report)
				.build()
			sendEnvelope(envelope)
			log.trace("Heartbeat sent to control plane")
		} catch (e: Exception) {
			log.warn("Error sending heartbeat report: {}", e.message)
		}
	}

	fun flushTelemetry() {
		if (!connected.get()) return
		try {
			val records = ringBuffer.drain(maxBatchSize)
			if (records.isNotEmpty()) {
				val batch = TelemetryBatchReport.newBuilder()
					.addAllRecords(records)
					.build()

				val envelope = AgentEnvelope.newBuilder()
					.setTraceId(UUID.randomUUID().toString())
					.setTelemetryBatch(batch)
					.build()

				sendEnvelope(envelope)
				log.debug("Flushed {} telemetry record(s) to control plane", records.size)
			}
		} catch (e: Exception) {
			log.warn("Error flushing telemetry batch: {}", e.message)
		}
	}

	@Synchronized
	private fun sendEnvelope(envelope: AgentEnvelope) {
		val stream = requestStream
		if (stream != null && connected.get()) {
			stream.onNext(envelope)
		}
	}

	companion object {
		private fun buildChannel(properties: ControlPlaneProperties): ManagedChannel {
			val parts = properties.endpoint.split(":")
			val host = parts[0]
			val port = if (parts.size > 1) parts[1].toInt() else if (properties.tls.enabled) 443 else 80

			val builder = ManagedChannelBuilder.forAddress(host, port)
			if (properties.tls.enabled) {
				builder.useTransportSecurity()
			} else {
				builder.usePlaintext()
			}
			return builder.build()
		}
	}
}
