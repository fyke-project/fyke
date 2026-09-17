package dev.fyke.benchmark.harness

import org.HdrHistogram.Histogram
import java.util.concurrent.TimeUnit

/**
 * Precision latency metrics collector utilizing HdrHistogram.
 *
 * Tracks commit-to-ack latency distribution without GC skew or coordinated omission.
 */
class LatencyTracker(
	private val highestTrackableValueNanos: Long = TimeUnit.MINUTES.toNanos(5),
	private val numberOfSignificantValueDigits: Int = 3
) {
	private val histogram = Histogram(highestTrackableValueNanos, numberOfSignificantValueDigits)

	@Synchronized
	fun recordNanos(nanos: Long) {
		val clamped = nanos.coerceIn(1L, highestTrackableValueNanos)
		histogram.recordValue(clamped)
	}

	@Synchronized
	fun recordMillis(millis: Long) {
		recordNanos(TimeUnit.MILLISECONDS.toNanos(millis))
	}

	@Synchronized
	fun p50Millis(): Double = histogram.getValueAtPercentile(50.0) / 1_000_000.0

	@Synchronized
	fun p90Millis(): Double = histogram.getValueAtPercentile(90.0) / 1_000_000.0

	@Synchronized
	fun p95Millis(): Double = histogram.getValueAtPercentile(95.0) / 1_000_000.0

	@Synchronized
	fun p99Millis(): Double = histogram.getValueAtPercentile(99.0) / 1_000_000.0

	@Synchronized
	fun p999Millis(): Double = histogram.getValueAtPercentile(99.9) / 1_000_000.0

	@Synchronized
	fun maxMillis(): Double = histogram.maxValue / 1_000_000.0

	@Synchronized
	fun meanMillis(): Double = histogram.mean / 1_000_000.0

	@Synchronized
	fun totalCount(): Long = histogram.totalCount

	@Synchronized
	fun reset() {
		histogram.reset()
	}

	@Synchronized
	fun generateReport(title: String, durationMs: Long): String {
		val total = histogram.totalCount
		val throughput = if (durationMs > 0) (total.toDouble() / (durationMs.toDouble() / 1000.0)) else 0.0

		return buildString {
			appendLine("=".repeat(64))
			appendLine(" BENCHMARK REPORT: $title")
			appendLine("=".repeat(64))
			appendLine("  Total Events Processed : $total")
			appendLine("  Elapsed Duration       : $durationMs ms")
			appendLine("  Throughput             : ${"%.2f".format(throughput)} msg/s")
			appendLine("-".repeat(64))
			appendLine("  Commit-to-ACK Latency Percentiles:")
			appendLine("    p50 (Median)         : ${"%.2f".format(p50Millis())} ms")
			appendLine("    p90                  : ${"%.2f".format(p90Millis())} ms")
			appendLine("    p95                  : ${"%.2f".format(p95Millis())} ms")
			appendLine("    p99                  : ${"%.2f".format(p99Millis())} ms")
			appendLine("    p99.9                : ${"%.2f".format(p999Millis())} ms")
			appendLine("    Max                  : ${"%.2f".format(maxMillis())} ms")
			appendLine("    Mean                 : ${"%.2f".format(meanMillis())} ms")
			appendLine("=".repeat(64))
		}
	}
}
