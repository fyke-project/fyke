package dev.fyke.core.partition

import dev.fyke.core.model.OutboxEvent
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class PartitionResolverTest {

	@Test
	fun `SinglePartitionResolver should always return default partition`() {
		val resolver = SinglePartitionResolver()
		val event1 = OutboxEvent(type = "OrderCreated", destination = "orders", businessKey = "ord-1", payload = "{}")
		val event2 = OutboxEvent(type = "UserCreated", destination = "users", businessKey = "usr-2", payload = "{}")

		assertThat(resolver.resolvePartition(event1)).isEqualTo("default")
		assertThat(resolver.resolvePartition(event2)).isEqualTo("default")
	}

	@Test
	fun `BusinessKeyPartitionResolver should resolve partition to businessKey`() {
		val resolver = BusinessKeyPartitionResolver()
		val event1 = OutboxEvent(type = "OrderCreated", destination = "orders", businessKey = "ord-123", payload = "{}")
		val event2 = OutboxEvent(type = "OrderCreated", destination = "orders", businessKey = "ord-456", payload = "{}")

		assertThat(resolver.resolvePartition(event1)).isEqualTo("ord-123")
		assertThat(resolver.resolvePartition(event2)).isEqualTo("ord-456")
	}
}
