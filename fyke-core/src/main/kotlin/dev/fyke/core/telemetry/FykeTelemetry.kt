package dev.fyke.core.telemetry

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.LongHistogram
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.Tracer
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong

/**
 * Telemetry provider for Fyke outbox operations (R7).
 *
 * Emits OpenTelemetry spans and metrics, safe for no-op execution when no OpenTelemetry SDK is configured.
 */
class FykeTelemetry(
	openTelemetry: OpenTelemetry? = null,
	val sanitizer: ClientSideSanitizer = ClientSideSanitizer()
) {
	private val log = LoggerFactory.getLogger(javaClass)

	private val tracer: Tracer = try {
		(openTelemetry ?: GlobalOpenTelemetry.get()).getTracer("dev.fyke", "0.1.0")
	} catch (e: Throwable) {
		log.debug("OpenTelemetry not available, falling back to no-op tracer: {}", e.message)
		OpenTelemetry.noop().getTracer("dev.fyke")
	}

	private val meter = try {
		(openTelemetry ?: GlobalOpenTelemetry.get()).getMeter("dev.fyke")
	} catch (e: Throwable) {
		OpenTelemetry.noop().getMeter("dev.fyke")
	}

	// Metrics (R7)
	private val publishedCounter: LongCounter = meter.counterBuilder("fyke.outbox.published")
		.setDescription("Total count of successfully published outbox events")
		.build()

	private val publishFailureCounter: LongCounter = meter.counterBuilder("fyke.outbox.failed")
		.setDescription("Total count of failed publish attempts")
		.build()

	private val dlqCounter: LongCounter = meter.counterBuilder("fyke.dlq.growth")
		.setDescription("Total count of messages routed to the DLQ")
		.build()

	private val publishLatencyHistogram: LongHistogram = meter.histogramBuilder("fyke.outbox.latency.ms")
		.ofLongs()
		.setDescription("Latency from event creation to broker publication confirm")
		.build()

	private val backlogGauge = AtomicLong(0)

	init {
		meter.gaugeBuilder("fyke.outbox.backlog.depth")
			.ofLongs()
			.setDescription("Current number of pending outbox events awaiting dispatch")
			.buildWithCallback { measurement -> measurement.record(backlogGauge.get()) }
	}

	fun recordPublished(durationMs: Long) {
		publishedCounter.add(1)
		publishLatencyHistogram.record(durationMs)
	}

	fun recordPublishFailure() {
		publishFailureCounter.add(1)
	}

	fun recordDlqMessage() {
		dlqCounter.add(1)
	}

	fun updateBacklogDepth(depth: Long) {
		backlogGauge.set(depth)
	}

	fun <T> recordSpan(spanName: String, attributes: Map<String, String> = emptyMap(), block: (Span) -> T): T {
		val span = tracer.spanBuilder(spanName).startSpan()
		val sanitized = sanitizer.sanitizeAttributes(attributes)
		sanitized.forEach { (k, v) -> span.setAttribute(k, v) }
		return try {
			block(span)
		} catch (t: Throwable) {
			span.recordException(t)
			throw t
		} finally {
			span.end()
		}
	}
}
