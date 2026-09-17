package dev.fyke.benchmark.harness

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

/**
 * Thread-safe verifier for per-partition strictly monotonic ordering and message loss detection.
 */
class FifoSequenceVerifier {
	private val partitionSequences = ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>>()
	private val totalReceived = AtomicInteger(0)

	fun record(partitionKey: String, sequenceNumber: Long) {
		partitionSequences.computeIfAbsent(partitionKey) { ConcurrentLinkedQueue() }.add(sequenceNumber)
		totalReceived.incrementAndGet()
	}

	fun totalEvents(): Int = totalReceived.get()

	fun partitionCount(): Int = partitionSequences.size

	fun verifyStrictFifo(): VerificationResult {
		var violations = 0
		var count = 0
		val partitionErrors = mutableListOf<String>()

		for ((partition, seqs) in partitionSequences) {
			var last = -1L
			for (seq in seqs) {
				count++
				if (seq <= last) {
					violations++
					partitionErrors.add("Partition '$partition': out-of-order sequence $seq after $last")
				}
				last = seq
			}
		}

		return VerificationResult(
			totalEvents = count,
			partitionCount = partitionSequences.size,
			outOfOrderCount = violations,
			isStrictFifo = (violations == 0),
			errorDetails = partitionErrors
		)
	}

	fun reset() {
		partitionSequences.clear()
		totalReceived.set(0)
	}
}

data class VerificationResult(
	val totalEvents: Int,
	val partitionCount: Int,
	val outOfOrderCount: Int,
	val isStrictFifo: Boolean,
	val errorDetails: List<String>
)
