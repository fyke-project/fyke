package dev.fyke.core.poller

import kotlin.math.pow
import kotlin.random.Random

/**
 * Exponential backoff calculation policy with configurable multiplier and jitter.
 */
data class BackoffPolicy(
	val initialBackoffMs: Long = 1000L,
	val backoffMultiplier: Double = 1.5,
	val withJitter: Boolean = true
) {
	/**
	 * Calculates backoff in milliseconds for the given attempt number (1-based).
	 */
	fun calculate(attempt: Int): Long {
		val exponent = (attempt - 1).coerceAtLeast(0).toDouble()
		val multiplier = backoffMultiplier.pow(exponent)
		val jitter = if (withJitter) Random.nextDouble(0.8, 1.2) else 1.0
		return (initialBackoffMs * multiplier * jitter).toLong()
	}
}
