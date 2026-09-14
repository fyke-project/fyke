package dev.fyke.core.partition

import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class SingleWorkerPartitionLockerTest {

	@Test
	fun `should acquire and release lock for partition`() {
		val locker = SingleWorkerPartitionLocker()
		val conn = mockk<Connection>(relaxed = true)

		val acquired = locker.tryLock("tenant-1", conn)
		assertThat(acquired).isTrue()

		// Same thread can re-acquire ReentrantLock
		assertThat(locker.tryLock("tenant-1", conn)).isTrue()

		locker.unlock("tenant-1", conn)
		locker.unlock("tenant-1", conn)
	}

	@Test
	fun `should block other threads from acquiring the same partition concurrently`() {
		val locker = SingleWorkerPartitionLocker()
		val conn = mockk<Connection>(relaxed = true)

		val latch = CountDownLatch(1)
		val executor = Executors.newSingleThreadExecutor()

		// Acquire in current thread
		assertThat(locker.tryLock("partition-A", conn)).isTrue()

		// Attempt acquire in background thread
		executor.submit {
			val backgroundAcquired = locker.tryLock("partition-A", conn)
			assertThat(backgroundAcquired).isFalse()

			// Different partition should be acquired successfully
			val otherPartitionAcquired = locker.tryLock("partition-B", conn)
			assertThat(otherPartitionAcquired).isTrue()
			locker.unlock("partition-B", conn)

			latch.countDown()
		}

		assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue()
		locker.unlock("partition-A", conn)
		executor.shutdown()
	}
}
