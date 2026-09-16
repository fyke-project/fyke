package dev.fyke.demo

import dev.fyke.core.model.FykeRecordSummary
import dev.fyke.core.model.OutboxRecord
import dev.fyke.starter.Fyke
import java.util.*
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
class DemoController(
	private val orderService: OrderService,
) {

	@PostMapping("/orders")
	fun createOrder(@RequestBody order: OrderRequest, @RequestParam(required = false) poison: Boolean = true) {
		orderService.createOrder(
			orderId = UUID.randomUUID().toString() + if (poison) "poison" else "",
			customer = order.customer,
			amount = order.amount,
		)
	}

	@PostMapping("/send")
	@Transactional
	fun send(
		@RequestParam(required = false) tenant: String = "000001",
		@RequestParam(required = false) orderId: String = UUID.randomUUID().toString(),
	): OutboxRecord {
		val id = UUID.randomUUID().toString()
		return Fyke.send(
			type = "order",
			destination = DemoApplication.DESTINATION,
			target = DemoApplication.TARGET,
			businessKey = id,
			payload = OrderCreatedPayload(
				orderId = id,
				customer = "test-customer",
				amount = 42.42,
			),
			partitionKey = tenant,
			headers = mapOf(
				"tenant" to tenant,
			),
		)
	}

	@PostMapping("/replay/{id}")
	fun replay(@PathVariable id: UUID): Boolean {
		return Fyke.replay(id)
	}

	@PostMapping("/retry-inbox/{id}")
	fun retryInbox(@PathVariable id: UUID): Boolean {
		return Fyke.retryInbox(id)
	}

	@GetMapping("/{id}")
	fun getById(@PathVariable id: String): List<FykeRecordSummary> {
		return Fyke.searchByBusinessKey(id)
	}


}

data class OrderRequest(
	val customer: String,
	val amount: Double,
)
