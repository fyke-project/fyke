package dev.fyke.exporter.controlplane.command

import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import dev.fyke.controlplane.v1.CommandEnvelope
import dev.fyke.controlplane.v1.CommandExecutionResult
import dev.fyke.controlplane.v1.CommandOutcome
import dev.fyke.controlplane.v1.EventChannel
import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.exporter.controlplane.config.ControlPlaneProperties
import dev.fyke.starter.Fyke
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.UUID

class LocalCommandExecutor(
	private val properties: ControlPlaneProperties,
	private val verifier: ControlPlaneCommandVerifier,
	private val outboxStore: OutboxStore?,
	private val inboxStore: InboxStore?
) {
	private val log = LoggerFactory.getLogger(javaClass)

	fun execute(envelope: CommandEnvelope): CommandExecutionResult {
		val commandId = envelope.commandId

		// 1. Signature Verification
		if (!verifier.verify(envelope)) {
			log.warn("Command rejected: unauthorized or invalid signature for commandId={}", commandId)
			return buildResult(commandId, CommandOutcome.COMMAND_OUTCOME_UNAUTHORIZED, "Signature verification failed")
		}

		// 2. Command Execution
		return try {
			when (envelope.commandCase) {
				CommandEnvelope.CommandCase.REPLAY_OUTBOX -> {
					val id = parseUuid(envelope.replayOutbox.outboxId)
					val success = Fyke.replayOutbox(id)
					if (success) {
						log.info("ReplayOutbox succeeded for record id={}", id)
						buildResult(commandId, CommandOutcome.COMMAND_OUTCOME_SUCCESS)
					} else {
						log.warn("ReplayOutbox failed or record not found for id={}", id)
						buildResult(commandId, CommandOutcome.COMMAND_OUTCOME_NOT_FOUND, "Record not found or replay rejected")
					}
				}

				CommandEnvelope.CommandCase.RETRY_OUTBOX -> {
					val id = parseUuid(envelope.retryOutbox.outboxId)
					val success = Fyke.replayOutbox(id)
					if (success) {
						log.info("RetryOutbox succeeded for record id={}", id)
						buildResult(commandId, CommandOutcome.COMMAND_OUTCOME_SUCCESS)
					} else {
						log.warn("RetryOutbox failed or record not found for id={}", id)
						buildResult(commandId, CommandOutcome.COMMAND_OUTCOME_NOT_FOUND, "Record not found or retry rejected")
					}
				}

				CommandEnvelope.CommandCase.RETRY_INBOX -> {
					val id = parseUuid(envelope.retryInbox.dlqId)
					val success = Fyke.retryInbox(id)
					if (success) {
						log.info("RetryInbox succeeded for record id={}", id)
						buildResult(commandId, CommandOutcome.COMMAND_OUTCOME_SUCCESS)
					} else {
						log.warn("RetryInbox failed or record not found for id={}", id)
						buildResult(commandId, CommandOutcome.COMMAND_OUTCOME_NOT_FOUND, "Inbox record not found or retry failed")
					}
				}

				CommandEnvelope.CommandCase.DISCARD_INBOX -> {
					val id = parseUuid(envelope.discardInbox.dlqId)
					val store = inboxStore
					if (store != null) {
						store.markCompleted(id, Instant.now())
						log.info("DiscardInbox marked record completed for id={} (reason='{}')", id, envelope.discardInbox.reason)
						buildResult(commandId, CommandOutcome.COMMAND_OUTCOME_SUCCESS)
					} else {
						buildResult(commandId, CommandOutcome.COMMAND_OUTCOME_FAILED, "InboxStore is not available")
					}
				}

				CommandEnvelope.CommandCase.FETCH_PAYLOAD -> {
					if (!properties.allowPayloadFetch) {
						log.warn("Payload fetch command rejected: allow-payload-fetch is false (ADR D-009)")
						buildResult(commandId, CommandOutcome.COMMAND_OUTCOME_DISABLED, "Payload fetch is disabled on this agent")
					} else {
						val id = parseUuid(envelope.fetchPayload.eventId)
						when (envelope.fetchPayload.channel) {
							EventChannel.EVENT_CHANNEL_OUTBOX -> {
								val record = outboxStore?.findOutboxById(id)
								if (record != null) {
									buildPayloadResult(commandId, record.payload, record.contentType)
								} else {
									buildResult(commandId, CommandOutcome.COMMAND_OUTCOME_NOT_FOUND, "Outbox record not found")
								}
							}
							EventChannel.EVENT_CHANNEL_INBOX -> {
								val record = inboxStore?.findInboxById(id)
								if (record != null) {
									buildPayloadResult(commandId, record.payload, record.contentType)
								} else {
									buildResult(commandId, CommandOutcome.COMMAND_OUTCOME_NOT_FOUND, "Inbox record not found")
								}
							}
							EventChannel.EVENT_CHANNEL_DLQ -> {
								val record = outboxStore?.findDlqById(id)
								if (record != null) {
									buildPayloadResult(commandId, record.payload, record.contentType)
								} else {
									buildResult(commandId, CommandOutcome.COMMAND_OUTCOME_NOT_FOUND, "DLQ record not found")
								}
							}
							else -> buildResult(commandId, CommandOutcome.COMMAND_OUTCOME_FAILED, "Unsupported event channel")
						}
					}
				}

				else -> {
					log.warn("Unsupported commandCase: {}", envelope.commandCase)
					buildResult(commandId, CommandOutcome.COMMAND_OUTCOME_FAILED, "Unsupported command type")
				}
			}
		} catch (e: Exception) {
			log.error("Exception executing commandId={}", commandId, e)
			buildResult(commandId, CommandOutcome.COMMAND_OUTCOME_FAILED, e.message ?: "Execution failed")
		}
	}

	private fun parseUuid(idStr: String): UUID {
		return UUID.fromString(idStr)
	}

	private fun buildResult(commandId: String, outcome: CommandOutcome, errorMessage: String? = null): CommandExecutionResult {
		val now = Instant.now()
		val builder = CommandExecutionResult.newBuilder()
			.setCommandId(commandId)
			.setOutcome(outcome)
			.setExecutedAt(
				Timestamp.newBuilder()
					.setSeconds(now.epochSecond)
					.setNanos(now.nano)
					.build()
			)
		if (errorMessage != null) {
			builder.setErrorMessage(errorMessage)
		}
		return builder.build()
	}

	private fun buildPayloadResult(commandId: String, payloadBytes: ByteArray, contentType: String): CommandExecutionResult {
		val now = Instant.now()
		return CommandExecutionResult.newBuilder()
			.setCommandId(commandId)
			.setOutcome(CommandOutcome.COMMAND_OUTCOME_SUCCESS)
			.setExecutedAt(
				Timestamp.newBuilder()
					.setSeconds(now.epochSecond)
					.setNanos(now.nano)
					.build()
			)
			.setRawPayload(ByteString.copyFrom(payloadBytes))
			.setPayloadContentType(contentType)
			.build()
	}
}
