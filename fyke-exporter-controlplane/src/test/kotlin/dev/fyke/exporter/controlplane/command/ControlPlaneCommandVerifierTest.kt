package dev.fyke.exporter.controlplane.command

import com.google.protobuf.Timestamp
import dev.fyke.controlplane.v1.CommandEnvelope
import dev.fyke.controlplane.v1.DiscardInboxCommand
import dev.fyke.controlplane.v1.ReplayOutboxCommand
import dev.fyke.controlplane.v1.RetryInboxCommand
import dev.fyke.controlplane.v1.RetryOutboxCommand
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.Signature
import java.time.Instant
import java.util.Base64
import java.util.UUID

class ControlPlaneCommandVerifierTest {

	private lateinit var keyPair: KeyPair
	private lateinit var publicKeyBase64: String
	private lateinit var verifier: ControlPlaneCommandVerifier

	@BeforeEach
	fun setUp() {
		val kpg = KeyPairGenerator.getInstance("Ed25519")
		keyPair = kpg.generateKeyPair()
		publicKeyBase64 = Base64.getEncoder().encodeToString(keyPair.public.encoded)
		verifier = ControlPlaneCommandVerifier(enabled = true, publicKeyBase64 = publicKeyBase64)
	}

	private fun sign(payload: ByteArray): String {
		val sig = Signature.getInstance("Ed25519")
		sig.initSign(keyPair.private)
		sig.update(payload)
		return Base64.getEncoder().encodeToString(sig.sign())
	}

	@Test
	fun `verify valid ReplayOutbox signature`() {
		val commandId = UUID.randomUUID().toString()
		val outboxId = UUID.randomUUID().toString()
		val businessKey = "ORDER-123"

		val signature = sign("$commandId:$outboxId:$businessKey".toByteArray())

		val envelope = CommandEnvelope.newBuilder()
			.setCommandId(commandId)
			.setSignature(signature)
			.setReplayOutbox(
				ReplayOutboxCommand.newBuilder()
					.setOutboxId(outboxId)
					.setBusinessKey(businessKey)
					.build()
			)
			.build()

		assertThat(verifier.verify(envelope)).isTrue()
	}

	@Test
	fun `verify valid RetryOutbox signature`() {
		val commandId = UUID.randomUUID().toString()
		val outboxId = UUID.randomUUID().toString()
		val businessKey = "ORDER-456"

		val signature = sign("$commandId:$outboxId:$businessKey".toByteArray())

		val envelope = CommandEnvelope.newBuilder()
			.setCommandId(commandId)
			.setSignature(signature)
			.setRetryOutbox(
				RetryOutboxCommand.newBuilder()
					.setOutboxId(outboxId)
					.setBusinessKey(businessKey)
					.build()
			)
			.build()

		assertThat(verifier.verify(envelope)).isTrue()
	}

	@Test
	fun `verify valid RetryInbox signature`() {
		val commandId = UUID.randomUUID().toString()
		val dlqId = UUID.randomUUID().toString()
		val businessKey = "INBOX-789"

		val signature = sign("$commandId:$dlqId:$businessKey".toByteArray())

		val envelope = CommandEnvelope.newBuilder()
			.setCommandId(commandId)
			.setSignature(signature)
			.setRetryInbox(
				RetryInboxCommand.newBuilder()
					.setDlqId(dlqId)
					.setBusinessKey(businessKey)
					.build()
			)
			.build()

		assertThat(verifier.verify(envelope)).isTrue()
	}

	@Test
	fun `verify valid DiscardInbox signature`() {
		val commandId = UUID.randomUUID().toString()
		val dlqId = UUID.randomUUID().toString()
		val reason = "Unprocessable poison message"

		val signature = sign("$commandId:$dlqId:$reason".toByteArray())

		val envelope = CommandEnvelope.newBuilder()
			.setCommandId(commandId)
			.setSignature(signature)
			.setDiscardInbox(
				DiscardInboxCommand.newBuilder()
					.setDlqId(dlqId)
					.setReason(reason)
					.build()
			)
			.build()

		assertThat(verifier.verify(envelope)).isTrue()
	}

	@Test
	fun `reject tampered payload or signature`() {
		val commandId = UUID.randomUUID().toString()
		val outboxId = UUID.randomUUID().toString()
		val businessKey = "ORDER-123"

		val signature = sign("$commandId:$outboxId:$businessKey".toByteArray())

		// Tampered outboxId
		val envelopeTampered = CommandEnvelope.newBuilder()
			.setCommandId(commandId)
			.setSignature(signature)
			.setReplayOutbox(
				ReplayOutboxCommand.newBuilder()
					.setOutboxId(UUID.randomUUID().toString()) // different ID!
					.setBusinessKey(businessKey)
					.build()
			)
			.build()

		assertThat(verifier.verify(envelopeTampered)).isFalse()
	}

	@Test
	fun `disabled verification always returns true`() {
		val disabledVerifier = ControlPlaneCommandVerifier(enabled = false, publicKeyBase64 = "")
		val envelope = CommandEnvelope.newBuilder()
			.setCommandId("cmd-1")
			.setSignature("invalid-signature")
			.build()

		assertThat(disabledVerifier.verify(envelope)).isTrue()
	}
}
