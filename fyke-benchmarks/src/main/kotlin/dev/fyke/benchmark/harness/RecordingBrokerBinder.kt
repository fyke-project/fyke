package dev.fyke.benchmark.harness

import dev.fyke.core.binder.BrokerBinder
import dev.fyke.core.binder.PublishResult
import dev.fyke.core.model.OutboxRecord
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * In-memory BrokerBinder implementation that isolates database lock contention and poller throughput
 * from external broker network/disk I/O.
 */
class RecordingBrokerBinder(
	var simulatedDelayMs: Long = 0L,
	var onPublishedCallback: ((OutboxRecord) -> Unit)? = null
) : BrokerBinder {

	val publishedRecords = CopyOnWriteArrayList<OutboxRecord>()
	val publishedCount = AtomicInteger(0)

	override fun name(): String = "recording"

	override fun publish(record: OutboxRecord): PublishResult {
		if (simulatedDelayMs > 0) {
			Thread.sleep(simulatedDelayMs)
		}
		publishedRecords.add(record)
		publishedCount.incrementAndGet()
		onPublishedCallback?.invoke(record)
		return PublishResult.Success
	}

	fun reset() {
		publishedRecords.clear()
		publishedCount.set(0)
	}
}
