package dev.fyke.core.inbox

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ConsumerPartitionResolverTest {

	private val resolver = DefaultConsumerPartitionResolver()

	@Test
	fun `should extract partition key from JSON payload when property is configured`() {
		val jsonPayload = """{"customerId": "cust-999", "orderId": "ord-123"}""".toByteArray()
		val headers = mapOf("x-fyke-partition-key" to "sender-key")

		val partition = resolver.resolve(headers, jsonPayload, "customerId")

		assertThat(partition).isEqualTo("cust-999")
	}

	@Test
	fun `should fallback to header when property is not configured`() {
		val jsonPayload = """{"customerId": "cust-999"}""".toByteArray()
		val headers = mapOf("x-fyke-partition-key" to "header-partition-1")

		val partition = resolver.resolve(headers, jsonPayload, null)

		assertThat(partition).isEqualTo("header-partition-1")
	}

	@Test
	fun `should fallback to business key header when partition key header is missing`() {
		val jsonPayload = "{}".toByteArray()
		val headers = mapOf("x-fyke-business-key" to "biz-key-456")

		val partition = resolver.resolve(headers, jsonPayload, "")

		assertThat(partition).isEqualTo("biz-key-456")
	}

	@Test
	fun `should fallback to default when no headers and no property match`() {
		val jsonPayload = "{}".toByteArray()
		val headers = emptyMap<String, String>()

		val partition = resolver.resolve(headers, jsonPayload, null)

		assertThat(partition).isEqualTo("default")
	}
}
