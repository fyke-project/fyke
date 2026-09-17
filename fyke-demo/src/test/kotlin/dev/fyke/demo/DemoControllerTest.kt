package dev.fyke.demo

import com.fasterxml.jackson.databind.ObjectMapper
import dev.fyke.starter.Fyke
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.UUID

@SpringBootTest
@Testcontainers
class DemoControllerTest {

	companion object {
		@Container
		@ServiceConnection
		val postgres = PostgreSQLContainer("postgres:16-alpine")

		@Container
		@ServiceConnection
		val rabbitmq = RabbitMQContainer("rabbitmq:3.13-management-alpine")
	}

	@Autowired
	private lateinit var wac: WebApplicationContext

	private lateinit var mockMvc: MockMvc

	@Autowired
	private lateinit var objectMapper: ObjectMapper

	@BeforeEach
	fun setUp() {
		mockMvc = MockMvcBuilders.webAppContextSetup(wac).build()
		mockMvc.perform(post("/api/reset"))
			.andExpect(status().isOk)
	}

	@Test
	fun `should create order via POST api orders and query status`() {
		val orderId = "order-api-${UUID.randomUUID()}"
		val request = CreateOrderRequest(
			orderId = orderId,
			customer = "Alice",
			amount = 150.0,
			failAttempts = 0,
		)

		mockMvc.perform(
			post("/api/orders")
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(request))
		)
			.andExpect(status().isCreated)
			.andExpect(jsonPath("$.orderId").value(orderId))
			.andExpect(jsonPath("$.businessKey").value(orderId))
			.andExpect(jsonPath("$.status").value("NEW"))

		// Check records search endpoint
		org.awaitility.kotlin.await.atMost(java.time.Duration.ofSeconds(10)).untilAsserted {
			mockMvc.perform(get("/api/records/$orderId"))
				.andExpect(status().isOk)
				.andExpect(jsonPath("$[0].businessKey").value(orderId))
		}
	}

	@Test
	fun `should return demo status`() {
		mockMvc.perform(get("/api/status"))
			.andExpect(status().isOk)
			.andExpect(jsonPath("$.activeBinder").value("rabbitmq"))
			.andExpect(jsonPath("$.outboxPendingCount").isNumber)
			.andExpect(jsonPath("$.inboxPendingCount").isNumber)
	}

	@Test
	fun `should create orders in burst via POST api test fifo-burst`() {
		val partition = "part-${UUID.randomUUID().toString().take(6)}"

		mockMvc.perform(
			post("/api/test/fifo-burst")
				.param("partitionKey", partition)
				.param("count", "2")
				.param("firstOrderFailAttempts", "1")
		)
			.andExpect(status().isOk)
			.andExpect(jsonPath("$.partitionKey").value(partition))
			.andExpect(jsonPath("$.firstOrderFailAttempts").value(1))
			.andExpect(jsonPath("$.orderIds").isArray)
	}

	@Test
	fun `should send event via POST api events`() {
		val key = "event-key-${UUID.randomUUID()}"
		val request = SendEventRequest(
			businessKey = key,
			type = "CustomEvent",
		)

		mockMvc.perform(
			post("/api/events")
				.contentType(MediaType.APPLICATION_JSON)
				.content(objectMapper.writeValueAsString(request))
		)
			.andExpect(status().isOk)
			.andExpect(jsonPath("$.businessKey").value(key))
	}
}
