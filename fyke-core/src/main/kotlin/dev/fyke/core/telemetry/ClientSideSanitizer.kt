package dev.fyke.core.telemetry

import java.security.MessageDigest

/**
 * Client-side sanitization configuration and utility (R6, ADR D-009).
 *
 * Ensures raw domain payloads never leave the JVM in telemetry, and sanitizes header/attribute maps
 * according to allow/deny rules.
 */
class ClientSideSanitizer(
	private val allowList: Set<String> = emptySet(),
	private val denyList: Set<String> = emptySet(),
	private val metadataOnly: Boolean = true
) {
	/**
	 * Sanitizes a map of headers/attributes.
	 * Deny-listed keys have their values hashed.
	 * If an allow-list is defined, only allowed keys are retained.
	 */
	fun sanitizeAttributes(attributes: Map<String, String>?): Map<String, String> {
		if (attributes.isNullOrEmpty()) return emptyMap()

		return attributes.filter { (key, _) ->
			if (allowList.isNotEmpty()) key in allowList else true
		}.mapValues { (key, value) ->
			if (key in denyList) hash(value) else value
		}
	}

	/**
	 * Determines whether raw payload bytes are permitted to leave the JVM.
	 * Defaults to false (metadata-only).
	 */
	fun isPayloadExportPermitted(): Boolean = !metadataOnly

	fun hash(value: String): String {
		val digest = MessageDigest.getInstance("SHA-256")
		return digest.digest(value.toByteArray(Charsets.UTF_8)).joinToString("") { "%02x".format(it) }
	}
}
