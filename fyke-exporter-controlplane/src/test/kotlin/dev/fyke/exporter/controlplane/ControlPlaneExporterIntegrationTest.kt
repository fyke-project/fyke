package dev.fyke.exporter.controlplane

import com.google.protobuf.Timestamp
import dev.fyke.controlplane.v1.AgentEnvelope
import dev.fyke.controlplane.v1.CommandEnvelope
import dev.fyke.controlplane.v1.CommandExecutionResult
import dev.fyke.controlplane.v1.CommandOutcome
import dev.fyke.controlplane.v1.ControlPlaneEnvelope
import dev.fyke.controlplane.v1.EventMetadataRecord
import dev.fyke.controlplane.v1.FykeControlPlaneServiceGrpc
import dev.fyke.controlplane.v1.HandshakeResponse
import dev.fyke.controlplane.v1.HeartbeatAck
import dev.fyke.controlplane.v1.ReplayOutboxCommand
import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.outbox.OutboxPollerEngine
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.core.outbox.OutboxWriter
import dev.fyke.exporter.controlplane.command.ControlPlaneCommandVerifier
import dev.fyke.exporter.controlplane.command.LocalCommandExecutor
import dev.fyke.exporter.controlplane.config.ControlPlaneProperties
import dev.fyke.exporter.controlplane.grpc.ControlPlaneGrpcClient
import dev.fyke.exporter.controlplane.heartbeat.ControlPlaneHeartbeatReporter
import dev.fyke.exporter.controlplane.telemetry.TelemetryRingBuffer
import dev.fyke.starter.Fyke
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.stub.StreamObserver
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.Signature
import java.time.Instant
import java.util.Base64
import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit

class ControlPlaneExporterIntegrationTest {

	private lateinit var server: Server
	private lateinit var channel: ManagedChannel
	private lateinit var keyPair: KeyPair
	private lateinit var publicKeyBase64: String

	private val receivedEnvelopes = ArrayBlockingQueue<AgentEnvelope>(50)
	private val receivedCommandResults = ArrayBlockingQueue<CommandExecutionResult>(10)
	private var controlPlaneStream: StreamObserver<ControlPlaneEnvelope>? = null

	private lateinit var mockOutboxStore: OutboxStore
	private lateinit var mockOutboxWriter: OutboxWriter
	private lateinit var mockOutboxPoller: OutboxPollerEngine
	private lateinit var mockInboxStore: InboxStore

	private lateinit var client: ControlPlaneGrpcClient
	private lateinit var ringBuffer: TelemetryRingBuffer
	private val serverName = InProcessServerBuilder.generateName()

	@BeforeEach
	fun setUp() {
		val kpg = KeyPairGenerator.getInstance("Ed25519")
		keyPair = kpg.generateKeyPair()
		publicKeyBase64 = Base64.getEncoder().encodeToString(keyPair.public.encoded)

		val serviceImpl = object : FykeControlPlaneServiceGrpc.FykeControlPlaneServiceImplBase() {
			override fun connectStream(
				responseObserver: StreamObserver<ControlPlaneEnvelope>
			): StreamObserver<AgentEnvelope> {
				controlPlaneStream = responseObserver

				return object : StreamObserver<AgentEnvelope> {
					override fun onNext(value: AgentEnvelope) {
						receivedEnvelopes.add(value)

						when (value.payloadCase) {
							AgentEnvelope.PayloadCase.HANDSHAKE -> {
								responseObserver.onNext(
									ControlPlaneEnvelope.newBuilder()
										.setCorrelationId(value.traceId)
										.setHandshakeAck(
											HandshakeResponse.newBuilder()
												.setAccepted(true)
												.setSessionId("session-test-123")
												.setHeartbeatIntervalSeconds(1)
												.setTelemetryFlushIntervalMs(50)
												.setTelemetryMaxBatchSize(100)
												.build()
										)
										.build()
								)
							}

							AgentEnvelope.PayloadCase.HEARTBEAT -> {
								responseObserver.onNext(
									ControlPlaneEnvelope.newBuilder()
										.setCorrelationId(value.traceId)
										.setHeartbeatAck(
											HeartbeatAck.newBuilder()
												.setAcknowledgedAt(
													Timestamp.newBuilder()
														.setSeconds(Instant.now().epochSecond)
														.build()
												)
												.build()
										)
										.build()
								)
							}

							AgentEnvelope.PayloadCase.COMMAND_RESULT -> {
								receivedCommandResults.add(value.commandResult)
							}

							else -> {}
						}
					}

					override fun onError(t: Throwable) {}
					override fun onCompleted() {
						responseObserver.onCompleted()
					}
				}
			}
		}

		server = InProcessServerBuilder.forName(serverName)
			.directExecutor()
			.addService(serviceImpl)
			.build()
			.start()

		channel = InProcessChannelBuilder.forName(serverName)
			.directExecutor()
			.build()

		mockOutboxStore = mockk(relaxed = true)
		mockOutboxWriter = mockk(relaxed = true)
		mockOutboxPoller = mockk(relaxed = true)
		mockInboxStore = mockk(relaxed = true)

		every { mockOutboxStore.countPending() } returns 5L
		every { mockOutboxStore.countUnreplayedDlq() } returns 1L
		every { mockInboxStore.countPending() } returns 2L

		Fyke.initialize(
			writer = mockOutboxWriter,
			outboxPoller = mockOutboxPoller,
			store = mockOutboxStore,
			inboxStore = mockInboxStore
		)

		val properties = ControlPlaneProperties(
			enabled = true,
			endpoint = "inprocess:0",
			apiKey = "test-token",
			tenantId = "tenant-xyz",
			appName = "order-service",
			environment = "staging",
			commandVerification = ControlPlaneProperties.CommandVerificationProperties(
				enabled = true,
				publicKey = publicKeyBase64
			),
			buffer = ControlPlaneProperties.BufferProperties(
				capacity = 1000,
				flushIntervalMs = 50,
				batchSize = 100
			),
			heartbeatIntervalSeconds = 1
		)

		ringBuffer = TelemetryRingBuffer(capacity = 1000)
		val heartbeatReporter = ControlPlaneHeartbeatReporter(mockOutboxStore, mockInboxStore)
		val verifier = ControlPlaneCommandVerifier(enabled = true, publicKeyBase64 = publicKeyBase64)
		val commandExecutor = LocalCommandExecutor(properties, verifier, mockOutboxStore, mockInboxStore)

		client = ControlPlaneGrpcClient(
			properties = properties,
			ringBuffer = ringBuffer,
			heartbeatReporter = heartbeatReporter,
			commandExecutor = commandExecutor,
			managedChannel = channel
		)
	}

	@AfterEach
	fun tearDown() {
		client.stop()
		channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS)
		server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS)
	}

	private fun sign(payload: ByteArray): String {
		val sig = Signature.getInstance("Ed25519")
		sig.initSign(keyPair.private)
		sig.update(payload)
		return Base64.getEncoder().encodeToString(sig.sign())
	}

	@Test
	fun `full lifecycle - handshake, telemetry stream, heartbeat, and remote replay execution`() {
		client.start()

		// 1. Verify Handshake
		val firstEnvelope = receivedEnvelopes.poll(5, TimeUnit.SECONDS)
		assertThat(firstEnvelope).isNotNull
		assertThat(firstEnvelope.payloadCase).isEqualTo(AgentEnvelope.PayloadCase.HANDSHAKE)
		assertThat(firstEnvelope.handshake.identity.tenantId).isEqualTo("tenant-xyz")
		assertThat(firstEnvelope.handshake.identity.appName).isEqualTo("order-service")

		// 2. Enqueue telemetry and verify streaming to control plane
		ringBuffer.enqueue(
			EventMetadataRecord.newBuilder()
				.setEventId("event-101")
				.setBusinessKey("ORD-101")
				.build()
		)

		var telemetryEnvelope: AgentEnvelope? = null
		val start = System.currentTimeMillis()
		while (System.currentTimeMillis() - start < 5000) {
			val env = receivedEnvelopes.poll(500, TimeUnit.MILLISECONDS) ?: continue
			if (env.payloadCase == AgentEnvelope.PayloadCase.TELEMETRY_BATCH) {
				telemetryEnvelope = env
				break
			}
		}

		assertThat(telemetryEnvelope).isNotNull
		assertThat(telemetryEnvelope!!.telemetryBatch.recordsList)
			.anyMatch { it.eventId == "event-101" && it.businessKey == "ORD-101" }

		// 3. Verify Heartbeat
		client.sendHeartbeat()
		var heartbeatEnvelope: AgentEnvelope? = null
		val hbStart = System.currentTimeMillis()
		while (System.currentTimeMillis() - hbStart < 5000) {
			val env = receivedEnvelopes.poll(500, TimeUnit.MILLISECONDS) ?: continue
			if (env.payloadCase == AgentEnvelope.PayloadCase.HEARTBEAT) {
				heartbeatEnvelope = env
				break
			}
		}

		assertThat(heartbeatEnvelope).isNotNull
		assertThat(heartbeatEnvelope!!.heartbeat.fykeOutboxPendingCount).isEqualTo(5L)
		assertThat(heartbeatEnvelope!!.heartbeat.fykeInboxPendingCount).isEqualTo(2L)

		// 4. Send signed ReplayOutboxCommand downstream from Control Plane
		val replayTargetId = UUID.randomUUID()
		every { mockOutboxPoller.replay(replayTargetId) } returns true

		val commandId = UUID.randomUUID().toString()
		val businessKey = "ORD-101"
		val signature = sign("$commandId:$replayTargetId:$businessKey".toByteArray())

		val commandEnvelope = ControlPlaneEnvelope.newBuilder()
			.setCorrelationId(UUID.randomUUID().toString())
			.setCommand(
				CommandEnvelope.newBuilder()
					.setCommandId(commandId)
					.setSignature(signature)
					.setReplayOutbox(
						ReplayOutboxCommand.newBuilder()
							.setOutboxId(replayTargetId.toString())
							.setBusinessKey(businessKey)
							.build()
					)
					.build()
			)
			.build()

		controlPlaneStream?.onNext(commandEnvelope)

		// 5. Verify agent executed replay in-JVM and sent back SUCCESS
		val result = receivedCommandResults.poll(5, TimeUnit.SECONDS)
		assertThat(result).isNotNull
		assertThat(result.commandId).isEqualTo(commandId)
		assertThat(result.outcome).isEqualTo(CommandOutcome.COMMAND_OUTCOME_SUCCESS)
	}
}
