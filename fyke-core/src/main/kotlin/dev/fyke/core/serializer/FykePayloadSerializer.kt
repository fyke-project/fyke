package dev.fyke.core.serializer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.security.MessageDigest

/**
 * SPI for serializing domain event objects into binary payloads stored in the database.
 */
interface FykePayloadSerializer {
	/** The MIME content type identifier (e.g. "application/json"). */
	fun contentType(): String

	/** Serializes the given payload object into raw bytes. */
	fun serialize(payload: Any): ByteArray

	/** Deserializes the raw bytes into an instance of targetType. */
	fun <T> deserialize(bytes: ByteArray, targetType: Class<T>): T

	/** Computes a cryptographic or integrity hash of the raw payload bytes. */
	fun computeHash(bytes: ByteArray): String {
		val digest = MessageDigest.getInstance("SHA-256")
		return digest.digest(bytes).joinToString("") { "%02x".format(it) }
	}
}

/**
 * Default Jackson-based JSON serializer supporting Kotlin data classes and Java 8 Time types.
 */
class JacksonFykePayloadSerializer(
	private val objectMapper: ObjectMapper = jacksonObjectMapper().apply {
		registerModule(JavaTimeModule())
		disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
	}
) : FykePayloadSerializer {

	override fun contentType(): String = "application/json"

	override fun serialize(payload: Any): ByteArray {
		return when (payload) {
			is ByteArray -> payload
			is String -> payload.toByteArray(Charsets.UTF_8)
			else -> objectMapper.writeValueAsBytes(payload)
		}
	}

	@Suppress("UNCHECKED_CAST")
	override fun <T> deserialize(bytes: ByteArray, targetType: Class<T>): T {
		return when {
			targetType == ByteArray::class.java -> bytes as T
			targetType == String::class.java -> String(bytes, Charsets.UTF_8) as T
			else -> objectMapper.readValue(bytes, targetType)
		}
	}
}
