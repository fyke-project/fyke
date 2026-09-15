package dev.fyke.core.poller

import dev.fyke.core.partition.SingleWorkerPartitionLocker
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import javax.sql.DataSource

class AbstractPollerEngineTest {

	private val partitionLocker = SingleWorkerPartitionLocker()
	private val dataSource = mockk<DataSource>()
	private val connection = mockk<Connection>(relaxed = true)

	@BeforeEach
	fun setUp() {
		every { dataSource.connection } returns connection
	}

	class TestPollerEngine(
		partitionLocker: SingleWorkerPartitionLocker,
		notificationSources: List<NotificationSource>,
		dataSource: DataSource,
		concurrency: Int = 1
	) : AbstractPollerEngine<String>(
		partitionLocker = partitionLocker,
		notificationSources = notificationSources,
		dataSource = dataSource,
		batchSize = 10,
		leaseDuration = Duration.ofSeconds(5),
		concurrency = concurrency,
		threadPrefix = "test-poller"
	) {
		val pendingPartitions = mutableListOf<String>()
		val batches = mutableMapOf<String, List<String>>()
		val processedRecords = CopyOnWriteArrayList<String>()

		override fun findPendingPartitions(): List<String> = pendingPartitions.toList()

		override fun claimBatch(partition: String): List<String> {
			return batches[partition] ?: emptyList()
		}

		override fun processRecord(record: String) {
			processedRecords.add(record)
		}
	}

	@Test
	fun `should poll and process partition batches sequentially`() {
		val poller = TestPollerEngine(partitionLocker, emptyList(), dataSource, concurrency = 1)
		poller.pendingPartitions.addAll(listOf("p1", "p2"))
		poller.batches["p1"] = listOf("rec-1", "rec-2")
		poller.batches["p2"] = listOf("rec-3")

		val count = poller.pollOnce()

		assertThat(count).isEqualTo(3)
		assertThat(poller.processedRecords).containsExactly("rec-1", "rec-2", "rec-3")
	}

	@Test
	fun `should process partitions concurrently with worker pool`() {
		val poller = TestPollerEngine(partitionLocker, emptyList(), dataSource, concurrency = 2)
		poller.start()

		poller.pendingPartitions.addAll(listOf("p1", "p2"))
		poller.batches["p1"] = listOf("rec-1")
		poller.batches["p2"] = listOf("rec-2")

		val count = poller.pollOnce()

		assertThat(count).isEqualTo(2)
		assertThat(poller.processedRecords).containsExactlyInAnyOrder("rec-1", "rec-2")

		poller.stop()
	}

	@Test
	fun `should trigger poll on notification wakeup`() {
		val latch = CountDownLatch(1)
		var triggerCallback: (() -> Unit)? = null

		val source = object : NotificationSource {
			override fun start(onWakeup: () -> Unit) {
				triggerCallback = onWakeup
			}
			override fun stop() {}
		}

		val poller = object : AbstractPollerEngine<String>(
			partitionLocker = partitionLocker,
			notificationSources = listOf(source),
			dataSource = dataSource,
			batchSize = 10,
			leaseDuration = Duration.ofSeconds(5),
			threadPrefix = "test-poller"
		) {
			var pollTriggered = false

			override fun findPendingPartitions(): List<String> {
				pollTriggered = true
				latch.countDown()
				return emptyList()
			}
			override fun claimBatch(partition: String): List<String> = emptyList()
			override fun processRecord(record: String) {}
		}

		poller.start()
		triggerCallback?.invoke()

		assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue()
		poller.stop()
	}
}
