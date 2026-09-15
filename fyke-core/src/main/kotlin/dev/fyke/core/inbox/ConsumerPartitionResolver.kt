package dev.fyke.core.inbox

import com.fasterxml.jackson.databind.ObjectMapper

/**
 * Strategy interface for resolving a partition key on consumed messages.
 */
fun interface ConsumerPartitionResolver {
	/**
	 * Resolves the partition key for an incoming message.
	 *
	 * @param headers Map of message headers.
	 * @param payloadBytes Raw binary payload.
	 * @param partitionKeyProperty Optional property name to extract from payload JSON.
	 * @return The resolved partition key.
	 */
	fun resolve(headers: Map<String, String>, payloadBytes: ByteArray, partitionKeyProperty: String?): String
}

/**
 * Default 3-tiered partition resolver for incoming messages:
 * 1. Property extraction if [partitionKeyProperty] is configured.
 * 2. Header fallback: 'x-fyke-partition-key', 'x-fyke-business-key', 'business_key', or 'correlation_id'.
 * 3. Default fallback: 'default'.
 */
class DefaultConsumerPartitionResolver(
	private val objectMapper: ObjectMapper = ObjectMapper()
) : ConsumerPartitionResolver {

	override fun resolve(
		headers: Map<String, String>,
		payloadBytes: ByteArray,
		partitionKeyProperty: String?
	): String {
		// Tier 1: Property extraction if explicitly configured
		if (!partitionKeyProperty.isNullOrBlank() && payloadBytes.isNotEmpty()) {
			try {
				val node = objectMapper.readTree(payloadBytes)
				val propValue = node.path(partitionKeyProperty).asText()
				if (!propValue.isNullOrBlank()) {
					return propValue
				}
			} catch (_: Exception) {
				// Fall back to headers on parse error
			}
		}

		// Tier 2: Headers fallback
		val headerKey = headers["x-fyke-partition-key"]
			?: headers["x-fyke-business-key"]
			?: headers["business_key"]
			?: headers["x-fyke-event-id"]
			?: headers["correlation_id"]

		if (!headerKey.isNullOrBlank()) {
			return headerKey
		}

		// Tier 3: Default partition
		return "default"
	}
}
