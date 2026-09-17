package dev.fyke.demo

import com.fasterxml.jackson.databind.ObjectMapper
import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.starter.actuator.FykeEndpoint
import io.micrometer.core.instrument.MeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@SpringBootTest
@Testcontainers
class FykeActuatorIntegrationTest {

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

	@Autowired
	private lateinit var meterRegistry: MeterRegistry

	@Autowired
	private lateinit var outboxStore: OutboxStore

	@Autowired
	private lateinit var inboxStore: InboxStore

	@BeforeEach
	fun setUp() {
		mockMvc = MockMvcBuilders.webAppContextSetup(wac).build()
	}

	@Test
	fun `actuator health endpoint should report fyke component as UP`() {
		mockMvc.perform(get("/actuator/health"))
			.andExpect(status().isOk)
			.andExpect(jsonPath("$.status").value("UP"))
			.andExpect(jsonPath("$.components.fyke.status").value("UP"))
			.andExpect(jsonPath("$.components.fyke.details.outbox.status").value("RUNNING"))
			.andExpect(jsonPath("$.components.fyke.details.outbox.channel").isString)
			.andExpect(jsonPath("$.components.fyke.details.outbox.pending").isNumber)
			.andExpect(jsonPath("$.components.fyke.details.inbox.status").value("RUNNING"))
			.andExpect(jsonPath("$.components.fyke.details.inbox.channel").isString)
			.andExpect(jsonPath("$.components.fyke.details.inbox.pending").isNumber)
			.andExpect(jsonPath("$.components.fyke.details.dlq.unreplayed").isNumber)
	}

	@Test
	fun `actuator fyke diagnostic endpoint should return operational snapshot`() {
		val result = mockMvc.perform(get("/actuator/fyke"))
			.andExpect(status().isOk)
			.andExpect(jsonPath("$.outbox.status").value("RUNNING"))
			.andExpect(jsonPath("$.outbox.batchSize").value(50))
			.andExpect(jsonPath("$.inbox.status").value("RUNNING"))
			.andExpect(jsonPath("$.inbox.batchSize").value(50))
			.andExpect(jsonPath("$.binder.name").value("rabbitmq"))
			.andExpect(jsonPath("$.retention.enabled").value(true))
			.andReturn()

		val json = result.response.contentAsString
		val snapshot = objectMapper.readValue(json, FykeEndpoint.FykeDiagnosticsSnapshot::class.java)
		assertThat(snapshot).isNotNull
		assertThat(snapshot.outbox.status).isEqualTo("RUNNING")
		assertThat(snapshot.inbox.status).isEqualTo("RUNNING")
	}

	@Test
	fun `micrometer metrics should bind fyke gauges`() {
		val outboxBacklog = meterRegistry.find("fyke.outbox.backlog").gauge()
		val outboxDead = meterRegistry.find("fyke.outbox.dead").gauge()
		val inboxBacklog = meterRegistry.find("fyke.inbox.backlog").gauge()
		val inboxDead = meterRegistry.find("fyke.inbox.dead").gauge()
		val dlqUnreplayed = meterRegistry.find("fyke.dlq.unreplayed").gauge()

		assertThat(outboxBacklog).isNotNull
		assertThat(outboxDead).isNotNull
		assertThat(inboxBacklog).isNotNull
		assertThat(inboxDead).isNotNull
		assertThat(dlqUnreplayed).isNotNull

		assertThat(outboxBacklog!!.value()).isGreaterThanOrEqualTo(0.0)
		assertThat(outboxDead!!.value()).isGreaterThanOrEqualTo(0.0)
		assertThat(inboxBacklog!!.value()).isGreaterThanOrEqualTo(0.0)
		assertThat(inboxDead!!.value()).isGreaterThanOrEqualTo(0.0)
		assertThat(dlqUnreplayed!!.value()).isGreaterThanOrEqualTo(0.0)
	}
}
