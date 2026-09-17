package dev.fyke.exporter.controlplane.command

import dev.fyke.controlplane.v1.CommandEnvelope
import org.slf4j.LoggerFactory
import java.security.KeyFactory
import java.security.PublicKey
import java.security.Signature
import java.security.spec.X509EncodedKeySpec
import java.util.Base64

class ControlPlaneCommandVerifier(
	private val enabled: Boolean,
	publicKeyBase64: String
) {
	private val log = LoggerFactory.getLogger(javaClass)

	private val publicKey: PublicKey? = if (enabled && publicKeyBase64.isNotBlank()) {
		try {
			val keyBytes = Base64.getDecoder().decode(publicKeyBase64.trim())
			val spec = X509EncodedKeySpec(keyBytes)
			val kf = KeyFactory.getInstance("Ed25519")
			kf.generatePublic(spec)
		} catch (e: Exception) {
			log.error("Failed to parse Ed25519 public key for command verification: {}", e.message)
			null
		}
	} else null

	fun verify(envelope: CommandEnvelope): Boolean {
		if (!enabled) {
			return true
		}
		val pubKey = publicKey
		if (pubKey == null) {
			log.warn("Command verification is enabled but no valid Ed25519 public key is configured")
			return false
		}

		val payloadToVerify = when (envelope.commandCase) {
			CommandEnvelope.CommandCase.REPLAY_OUTBOX -> {
				"${envelope.commandId}:${envelope.replayOutbox.outboxId}:${envelope.replayOutbox.businessKey}".toByteArray()
			}
			CommandEnvelope.CommandCase.RETRY_OUTBOX -> {
				"${envelope.commandId}:${envelope.retryOutbox.outboxId}:${envelope.retryOutbox.businessKey}".toByteArray()
			}
			CommandEnvelope.CommandCase.RETRY_INBOX -> {
				"${envelope.commandId}:${envelope.retryInbox.dlqId}:${envelope.retryInbox.businessKey}".toByteArray()
			}
			CommandEnvelope.CommandCase.DISCARD_INBOX -> {
				"${envelope.commandId}:${envelope.discardInbox.dlqId}:${envelope.discardInbox.reason}".toByteArray()
			}
			CommandEnvelope.CommandCase.FETCH_PAYLOAD -> {
				"${envelope.commandId}:${envelope.fetchPayload.channel}:${envelope.fetchPayload.eventId}".toByteArray()
			}
			else -> null
		} ?: return false

		return try {
			val sig = Signature.getInstance("Ed25519")
			sig.initVerify(pubKey)
			sig.update(payloadToVerify)
			val signatureBytes = Base64.getDecoder().decode(envelope.signature)
			sig.verify(signatureBytes)
		} catch (e: Exception) {
			log.warn("Signature verification failed for commandId={}: {}", envelope.commandId, e.message)
			false
		}
	}
}
