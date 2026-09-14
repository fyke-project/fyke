package dev.fyke.demo

import java.util.*
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

}

data class OrderRequest(
	val customer: String,
	val amount: Double,
)
