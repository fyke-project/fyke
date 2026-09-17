package dev.fyke.demo

import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.model.DlqRecord
import dev.fyke.core.model.FykeRecordSummary
import dev.fyke.core.model.InboxRecord
import dev.fyke.core.model.OutboxEvent
import dev.fyke.core.model.OutboxRecord
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.starter.Fyke
import java.util.UUID
import org.springframework.core.env.Environment
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
class DemoController(
	private val orderService: OrderService,
	private val outboxStore: OutboxStore,
	private val inboxStore: InboxStore,
	private val orderConsumer: OrderConsumer,
	private val orderInboxConsumer: OrderInboxConsumer,
	private val environment: Environment,
) {

	@PostMapping("/api/orders", "/orders")
	@ResponseStatus(HttpStatus.CREATED)
	fun createOrder(
		@RequestBody(required = false) body: CreateOrderRequest?,
		@RequestParam(required = false) customer: String?,
		@RequestParam(required = false) amount: Double?,
		@RequestParam(required = false) orderId: String?,
		@RequestParam(required = false) failAttempts: Int?,
		@RequestParam(required = false) fatal: Boolean?,
		@RequestParam(required = false) outboxFailAttempts: Int?,
		@RequestParam(required = false) outboxFatal: Boolean?,
		@RequestParam(required = false) poison: Boolean?,
		@RequestParam(required = false) partitionKey: String?,
	): OrderResponse {
		val req = body ?: CreateOrderRequest()
		val effectivePoison = poison ?: req.poison
		val effectiveOrderId = orderId ?: req.orderId ?: (UUID.randomUUID().toString() + if (effectivePoison) "poison" else "")
		val effectiveCustomer = customer ?: req.customer
		val effectiveAmount = amount ?: req.amount
		val effectiveFailAttempts = failAttempts ?: req.failAttempts
		val effectiveFatal = fatal ?: req.fatal
		val effectiveOutboxFailAttempts = outboxFailAttempts ?: req.outboxFailAttempts
		val effectiveOutboxFatal = outboxFatal ?: req.outboxFatal
		val effectivePartitionKey = partitionKey ?: req.partitionKey

		val record = orderService.createOrder(
			orderId = effectiveOrderId,
			customer = effectiveCustomer,
			amount = effectiveAmount,
			failAttempts = effectiveFailAttempts,
			fatal = effectiveFatal,
			outboxFailAttempts = effectiveOutboxFailAttempts,
			outboxFatal = effectiveOutboxFatal,
			partitionKey = effectivePartitionKey,
		)

		return OrderResponse(
			orderId = effectiveOrderId,
			outboxRecordId = record.id,
			status = record.status.name,
			businessKey = record.businessKey,
			destination = record.destination,
			target = record.target,
			partitionKey = record.partitionKey,
			failAttempts = effectiveFailAttempts,
			fatal = effectiveFatal,
			outboxFailAttempts = effectiveOutboxFailAttempts,
			outboxFatal = effectiveOutboxFatal,
		)
	}

	@PostMapping("/api/events", "/send")
	@Transactional
	fun send(
		@RequestBody(required = false) body: SendEventRequest?,
		@RequestParam(required = false) tenant: String?,
		@RequestParam(required = false) orderId: String?,
		@RequestParam(required = false) businessKey: String?,
		@RequestParam(required = false) partitionKey: String?,
		@RequestParam(required = false) destination: String?,
		@RequestParam(required = false) target: String?,
		@RequestParam(required = false) failAttempts: Int?,
		@RequestParam(required = false) fatal: Boolean?,
		@RequestParam(required = false) outboxFailAttempts: Int?,
		@RequestParam(required = false) outboxFatal: Boolean?,
	): OutboxRecord {
		val req = body ?: SendEventRequest()
		val effectiveKey = businessKey ?: orderId ?: req.businessKey ?: UUID.randomUUID().toString()
		val effectivePartition = partitionKey ?: tenant ?: req.partitionKey ?: "default"
		val effectiveDestination = destination ?: req.destination ?: DemoApplication.DESTINATION
		val effectiveTarget = target ?: req.target ?: DemoApplication.TARGET

		val effectiveFailAttempts = failAttempts ?: req.failAttempts
		val effectiveFatal = fatal ?: req.fatal
		val effectiveOutboxFailAttempts = outboxFailAttempts ?: req.outboxFailAttempts
		val effectiveOutboxFatal = outboxFatal ?: req.outboxFatal

		val headers = mutableMapOf<String, String>()
		req.headers?.let { headers.putAll(it) }
		if (effectivePartition.isNotBlank()) {
			headers["tenant"] = effectivePartition
		}
		if (effectiveFailAttempts > 0) {
			headers["x-fail-attempts"] = effectiveFailAttempts.toString()
		}
		if (effectiveFatal) {
			headers["x-fatal"] = "true"
		}
		if (effectiveOutboxFailAttempts > 0) {
			headers["x-fyke-simulate-outbox-fail"] = effectiveOutboxFailAttempts.toString()
		}
		if (effectiveOutboxFatal) {
			headers["x-fyke-simulate-outbox-dead"] = "true"
		}

		val payload = req.payload ?: OrderCreatedPayload(
			orderId = effectiveKey,
			customer = "api-customer",
			amount = 42.42,
			failAttempts = effectiveFailAttempts,
			fatal = effectiveFatal,
		)

		return Fyke.send(
			OutboxEvent(
				type = req.type,
				destination = effectiveDestination,
				target = effectiveTarget,
				businessKey = effectiveKey,
				partitionKey = effectivePartition,
				payload = payload,
				headers = headers.ifEmpty { null },
				idempotencyKey = req.idempotencyKey?.ifBlank { null } ?: UUID.randomUUID().toString(),
			)
		)
	}

	@GetMapping("/api/records/{businessKey}")
	fun searchRecords(@PathVariable businessKey: String): List<FykeRecordSummary> {
		return Fyke.searchByBusinessKey(businessKey)
	}

	@GetMapping("/{id}")
	fun getById(@PathVariable id: String): List<FykeRecordSummary> {
		return Fyke.searchByBusinessKey(id)
	}

	@GetMapping("/api/outbox/{id}")
	fun getOutboxById(@PathVariable id: UUID): ResponseEntity<OutboxRecord> {
		val record = outboxStore.findOutboxById(id)
		return if (record != null) ResponseEntity.ok(record) else ResponseEntity.notFound().build()
	}

	@GetMapping("/api/inbox/{id}")
	fun getInboxById(@PathVariable id: UUID): ResponseEntity<InboxRecord> {
		val record = inboxStore.findInboxById(id)
		return if (record != null) ResponseEntity.ok(record) else ResponseEntity.notFound().build()
	}

	@GetMapping("/api/dlq/{id}")
	fun getDlqById(@PathVariable id: UUID): ResponseEntity<DlqRecord> {
		val record = outboxStore.findDlqById(id)
		return if (record != null) ResponseEntity.ok(record) else ResponseEntity.notFound().build()
	}

	@GetMapping("/api/status")
	fun getStatus(): DemoStatusResponse {
		val activeProfiles = environment.activeProfiles.toList().ifEmpty { listOf("default (rabbitmq)") }
		val activeBinder = environment.getProperty("fyke.binder", "auto-detect")
		val failureCounters = orderInboxConsumer.failureCountPerOrder.mapValues { it.value.get() }

		return DemoStatusResponse(
			activeBinder = activeBinder,
			activeProfiles = activeProfiles,
			outboxPendingCount = outboxStore.countPending(),
			inboxPendingCount = inboxStore.countPending(),
			plainConsumerOrders = orderConsumer.receivedOrders.toList(),
			inboxConsumerOrders = orderInboxConsumer.receivedOrders.toList(),
			inboxFailureCounters = failureCounters,
		)
	}

	@PostMapping("/api/outbox/replay/{id}", "/replay/{id}")
	fun replay(@PathVariable id: UUID): Map<String, Any> {
		val replayed = Fyke.replay(id)
		return mapOf("replayed" to replayed, "id" to id)
	}

	@PostMapping("/api/inbox/retry/{id}", "/retry-inbox/{id}")
	fun retryInbox(@PathVariable id: UUID): Map<String, Any> {
		val retried = Fyke.retryInbox(id)
		return mapOf("retried" to retried, "id" to id)
	}

	@PostMapping("/api/test/fifo-burst")
	fun fifoBurst(
		@RequestParam(defaultValue = "tenant-burst") partitionKey: String,
		@RequestParam(defaultValue = "3") count: Int,
		@RequestParam(defaultValue = "2") firstOrderFailAttempts: Int,
	): Map<String, Any> {
		val orderIds = mutableListOf<String>()
		for (i in 1..count) {
			val orderId = "burst-$partitionKey-$i-${UUID.randomUUID().toString().take(6)}"
			val fails = if (i == 1) firstOrderFailAttempts else 0
			orderService.createOrder(
				orderId = orderId,
				customer = "Customer-$i",
				amount = i * 10.0,
				failAttempts = fails,
				partitionKey = partitionKey,
			)
			orderIds.add(orderId)
		}
		return mapOf(
			"message" to "Generated $count orders on partition '$partitionKey'",
			"partitionKey" to partitionKey,
			"firstOrderFailAttempts" to firstOrderFailAttempts,
			"orderIds" to orderIds,
		)
	}

	@PostMapping("/api/reset")
	fun reset(): Map<String, String> {
		orderConsumer.clear()
		orderInboxConsumer.clear()
		return mapOf("status" to "cleared")
	}
}

data class CreateOrderRequest(
	val customer: String = "Test Customer",
	val amount: Double = 99.99,
	val orderId: String? = null,
	val failAttempts: Int = 0,
	val fatal: Boolean = false,
	val outboxFailAttempts: Int = 0,
	val outboxFatal: Boolean = false,
	val poison: Boolean = false,
	val partitionKey: String? = null,
)

data class OrderResponse(
	val orderId: String,
	val outboxRecordId: UUID,
	val status: String,
	val businessKey: String,
	val destination: String,
	val target: String?,
	val partitionKey: String,
	val failAttempts: Int,
	val fatal: Boolean,
	val outboxFailAttempts: Int,
	val outboxFatal: Boolean,
)

data class SendEventRequest(
	val type: String = "OrderCreated",
	val destination: String? = null,
	val target: String? = null,
	val businessKey: String? = null,
	val partitionKey: String? = null,
	val payload: Any? = null,
	val headers: Map<String, String>? = null,
	val idempotencyKey: String? = null,
	val failAttempts: Int = 0,
	val fatal: Boolean = false,
	val outboxFailAttempts: Int = 0,
	val outboxFatal: Boolean = false,
)

data class DemoStatusResponse(
	val activeBinder: String,
	val activeProfiles: List<String>,
	val outboxPendingCount: Long,
	val inboxPendingCount: Long,
	val plainConsumerOrders: List<String>,
	val inboxConsumerOrders: List<String>,
	val inboxFailureCounters: Map<String, Int>,
)
