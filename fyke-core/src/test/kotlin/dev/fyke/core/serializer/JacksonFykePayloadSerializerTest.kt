package dev.fyke.core.serializer

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

class JacksonFykePayloadSerializerTest {

	private val serializer = JacksonFykePayloadSerializer()

	data class TestOrder(
		val orderId: String,
		val amount: Double,
		val createdAt: Instant
	)

	@Test
	fun `should serialize and deserialize Kotlin data class`() {
		val order = TestOrder("ord-99", 42.50, Instant.now())
		val bytes = serializer.serialize(order)

		assertThat(bytes).isNotEmpty()
		assertThat(serializer.contentType()).isEqualTo("application/json")

		val deserialized = serializer.deserialize(bytes, TestOrder::class.java)
		assertThat(deserialized).isEqualTo(order)
	}

	@Test
	fun `should handle raw ByteArray without re-serializing`() {
		val rawBytes = "hello raw bytes".toByteArray(Charsets.UTF_8)
		val result = serializer.serialize(rawBytes)

		assertThat(result).isEqualTo(rawBytes)
		val deserialized = serializer.deserialize(result, ByteArray::class.java)
		assertThat(deserialized).isEqualTo(rawBytes)
	}

	@Test
	fun `should compute consistent SHA-256 hash`() {
		val payload1 = "test-content".toByteArray(Charsets.UTF_8)
		val payload2 = "test-content".toByteArray(Charsets.UTF_8)
		val payload3 = "different-content".toByteArray(Charsets.UTF_8)

		assertThat(serializer.computeHash(payload1)).isEqualTo(serializer.computeHash(payload2))
		assertThat(serializer.computeHash(payload1)).isNotEqualTo(serializer.computeHash(payload3))
	}
}
