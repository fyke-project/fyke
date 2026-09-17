package dev.fyke.exporter.controlplane.config

import java.net.InetAddress
import java.util.*
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "fyke.control-plane")
data class ControlPlaneProperties(
	var enabled: Boolean = false,
	var endpoint: String = "localhost:9090",
	var apiKey: String = "",
	var tenantId: String = "default",
	var appName: String = "unknown",
	var environment: String = "development",
	var instanceId: String = defaultInstanceId(),
	var heartbeatIntervalSeconds: Int = 15,
	var allowPayloadFetch: Boolean = false,
	var tls: TlsProperties = TlsProperties(),
	var commandVerification: CommandVerificationProperties = CommandVerificationProperties(),
	var buffer: BufferProperties = BufferProperties(),
) {
	data class TlsProperties(
		var enabled: Boolean = false,
	)

	data class CommandVerificationProperties(
		var enabled: Boolean = true,
		var publicKey: String = "",
	)

	data class BufferProperties(
		var capacity: Int = 10000,
		var flushIntervalMs: Long = 1000,
		var batchSize: Int = 500,
	)

	companion object {
		private fun defaultInstanceId(): String {
			return try {
				InetAddress.getLocalHost().hostName
			} catch (e: Exception) {
				UUID.randomUUID().toString().take(8)
			}
		}


	}
}
