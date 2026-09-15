package dev.fyke.core.model

/**
 * Strategy for consumer-side event ordering within a partition upon failure.
 */
enum class OrderingMode {
	/**
	 * Earlier failing events pause subsequent events in the partition until retry or DLQ.
	 * Guarantees strict causal ordering within the partition.
	 */
	STRICT_FIFO,

	/**
	 * Subsequent events in the partition proceed even if an earlier event is in backoff retry.
	 * Suited for independent or commutative operations (e.g. notifications).
	 */
	LEAPFROG
}
