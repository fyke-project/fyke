package dev.fyke.core.telemetry

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ClientSideSanitizerTest {

	@Test
	fun `default metadata-only should not permit raw payload export`() {
		val sanitizer = ClientSideSanitizer()
		assertThat(sanitizer.isPayloadExportPermitted()).isFalse()
	}

	@Test
	fun `deny-listed attributes should be hashed`() {
		val sanitizer = ClientSideSanitizer(denyList = setOf("secret_key", "password"))
		val attributes = mapOf(
			"order_id" to "ord-123",
			"secret_key" to "my-super-secret",
			"tenant" to "acme"
		)

		val sanitized = sanitizer.sanitizeAttributes(attributes)

		assertThat(sanitized["order_id"]).isEqualTo("ord-123")
		assertThat(sanitized["tenant"]).isEqualTo("acme")
		assertThat(sanitized["secret_key"]).isNotEqualTo("my-super-secret")
		assertThat(sanitized["secret_key"]).isEqualTo(sanitizer.hash("my-super-secret"))
	}

	@Test
	fun `allow-list should only retain specified attributes`() {
		val sanitizer = ClientSideSanitizer(allowList = setOf("allowed_attr", "business_key"))
		val attributes = mapOf(
			"allowed_attr" to "value1",
			"business_key" to "key2",
			"internal_secret" to "sensitive"
		)

		val sanitized = sanitizer.sanitizeAttributes(attributes)

		assertThat(sanitized).containsOnlyKeys("allowed_attr", "business_key")
		assertThat(sanitized["internal_secret"]).isNull()
	}
}
