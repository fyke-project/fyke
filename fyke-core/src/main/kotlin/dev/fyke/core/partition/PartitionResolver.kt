package dev.fyke.core.partition

import dev.fyke.core.model.OutboxEvent

/**
 * Strategy interface for assigning outbox events to logical partitions.
 *
 * Events assigned to the same partition are guaranteed to be claimed and dispatched in strict sequential order.
 */
fun interface PartitionResolver {
	/**
	 * Resolves the partition key for the given domain event.
	 */
	fun resolvePartition(event: OutboxEvent): String
}

/**
 * Default partition resolver that assigns all events to a single global partition ("default").
 * Guarantees strict FIFO ordering across the entire system.
 */
class SinglePartitionResolver : PartitionResolver {
	override fun resolvePartition(event: OutboxEvent): String = "default"
}

/**
 * Partition resolver that partitions by the event's business key.
 * Guarantees strict FIFO ordering per entity / aggregate while allowing concurrent processing across distinct keys.
 */
class BusinessKeyPartitionResolver : PartitionResolver {
	override fun resolvePartition(event: OutboxEvent): String = event.businessKey
}
