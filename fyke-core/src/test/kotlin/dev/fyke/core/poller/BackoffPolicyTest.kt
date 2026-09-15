package dev.fyke.core.poller

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class BackoffPolicyTest {

	@Test
	fun `should calculate exponential backoff without jitter`() {
		val policy = BackoffPolicy(initialBackoffMs = 1000L, backoffMultiplier = 2.0, withJitter = false)

		assertThat(policy.calculate(1)).isEqualTo(1000L)
		assertThat(policy.calculate(2)).isEqualTo(2000L)
		assertThat(policy.calculate(3)).isEqualTo(4000L)
		assertThat(policy.calculate(4)).isEqualTo(8000L)
	}

	@Test
	fun `should apply jitter bounds within 20 percent`() {
		val policy = BackoffPolicy(initialBackoffMs = 1000L, backoffMultiplier = 2.0, withJitter = true)

		for (i in 1..100) {
			val backoff = policy.calculate(1)
			assertThat(backoff).isBetween(800L, 1200L)
		}
	}
}
