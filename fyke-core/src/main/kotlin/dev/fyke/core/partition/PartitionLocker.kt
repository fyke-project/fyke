package dev.fyke.core.partition

import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock

/**
 * Strategy interface for acquiring mutual exclusion on a partition across concurrent dispatchers.
 */
interface PartitionLocker {
	/**
	 * Attempts to acquire an exclusive lock on the partition for the active database transaction or worker.
	 *
	 * @return true if the lock was acquired, false if another worker holds the partition.
	 */
	fun tryLock(partitionKey: String, connection: Connection): Boolean

	/**
	 * Releases any explicit lock held on the partition.
	 */
	fun unlock(partitionKey: String, connection: Connection)
}

/**
 * PostgreSQL implementation using transaction-scoped advisory locks: `pg_try_advisory_xact_lock(hashtext(?))`.
 *
 * Locks are tied to the active database transaction and automatically released when the transaction
 * commits or rolls back, or if the connection closes (e.g. during a pod crash).
 */
class PostgresAdvisoryPartitionLocker : PartitionLocker {
	private val log = LoggerFactory.getLogger(javaClass)

	override fun tryLock(partitionKey: String, connection: Connection): Boolean {
		return try {
			connection.prepareStatement("SELECT pg_try_advisory_xact_lock(hashtext(?))").use { stmt ->
				stmt.setString(1, partitionKey)
				stmt.executeQuery().use { rs ->
					if (rs.next()) rs.getBoolean(1) else false
				}
			}
		} catch (e: Exception) {
			log.warn("Failed to acquire advisory lock for partition '{}': {}", partitionKey, e.message)
			false
		}
	}

	override fun unlock(partitionKey: String, connection: Connection) {
		// Transaction-scoped advisory locks automatically release on tx commit/rollback.
	}
}

/**
 * In-JVM fallback partition locker using ReentrantLocks for local testing (e.g. H2) or non-PostgreSQL databases.
 */
class SingleWorkerPartitionLocker : PartitionLocker {
	private val locks = ConcurrentHashMap<String, ReentrantLock>()

	override fun tryLock(partitionKey: String, connection: Connection): Boolean {
		val lock = locks.computeIfAbsent(partitionKey) { ReentrantLock() }
		return lock.tryLock()
	}

	override fun unlock(partitionKey: String, connection: Connection) {
		locks[partitionKey]?.let { lock ->
			if (lock.isHeldByCurrentThread) {
				lock.unlock()
			}
		}
	}
}
