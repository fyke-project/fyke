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

import dev.fyke.core.model.DlqRecord
import dev.fyke.core.model.InboxRecord
import dev.fyke.core.model.InboxStatus
import dev.fyke.core.model.OutboxRecord
import dev.fyke.core.model.OutboxStatus

/**
 * Telemetry provider for Fyke outbox operations (R7).
 *
 * Emits OpenTelemetry spans and metrics, safe for no-op execution when no OpenTelemetry SDK is configured.
 * Dispatches domain event lifecycle transitions to registered [FykeEventListener] instances.
 */
class FykeTelemetry(
	openTelemetry: OpenTelemetry? = null,
	val sanitizer: ClientSideSanitizer = ClientSideSanitizer(),
	private val listeners: List<FykeEventListener> = emptyList()
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
		log.trace("Fyke: Telemetry counter published +1, latency={} ms", durationMs)
		publishedCounter.add(1)
		publishLatencyHistogram.record(durationMs)
	}

	fun recordPublishFailure() {
		log.trace("Fyke: Telemetry counter publishFailure +1")
		publishFailureCounter.add(1)
	}

	fun recordDlqMessage() {
		log.trace("Fyke: Telemetry counter dlqGrowth +1")
		dlqCounter.add(1)
	}

	fun updateBacklogDepth(depth: Long) {
		log.trace("Fyke: Telemetry gauge backlogDepth={}", depth)
		backlogGauge.set(depth)
	}

	/**
	 * Returns the trace ID of the currently active OpenTelemetry span, or null if no valid span is active.
	 */
	fun currentTraceId(): String? {
		val spanContext = Span.current().spanContext
		return if (spanContext.isValid) spanContext.traceId else null
	}

	fun <T> recordSpan(spanName: String, attributes: Map<String, String> = emptyMap(), block: (Span) -> T): T {
		val span = tracer.spanBuilder(spanName).startSpan()
		val sanitized = sanitizer.sanitizeAttributes(attributes)
		sanitized.forEach { (k, v) -> span.setAttribute(k, v) }
		log.trace("Fyke: Recording telemetry span '{}' with {} attribute(s)", spanName, sanitized.size)
		val scope = span.makeCurrent()
		return try {
			block(span)
		} catch (t: Throwable) {
			span.recordException(t)
			throw t
		} finally {
			scope.close()
			span.end()
		}
	}

	fun notifyOutboxCreated(record: OutboxRecord) {
		if (listeners.isEmpty()) return
		for (listener in listeners) {
			try {
				listener.onOutboxCreated(record)
			} catch (t: Throwable) {
				log.warn("Fyke: FykeEventListener failed onOutboxCreated for id={}: {}", record.id, t.message)
			}
		}
	}

	fun notifyOutboxStatusChanged(
		record: OutboxRecord,
		oldStatus: OutboxStatus,
		newStatus: OutboxStatus,
		errorReason: String? = null
	) {
		if (listeners.isEmpty()) return
		for (listener in listeners) {
			try {
				listener.onOutboxStatusChanged(record, oldStatus, newStatus, errorReason)
			} catch (t: Throwable) {
				log.warn("Fyke: FykeEventListener failed onOutboxStatusChanged for id={}: {}", record.id, t.message)
			}
		}
	}

	fun notifyInboxReceived(record: InboxRecord) {
		if (listeners.isEmpty()) return
		for (listener in listeners) {
			try {
				listener.onInboxReceived(record)
			} catch (t: Throwable) {
				log.warn("Fyke: FykeEventListener failed onInboxReceived for id={}: {}", record.id, t.message)
			}
		}
	}

	fun notifyInboxStatusChanged(
		record: InboxRecord,
		oldStatus: InboxStatus,
		newStatus: InboxStatus,
		errorReason: String? = null
	) {
		if (listeners.isEmpty()) return
		for (listener in listeners) {
			try {
				listener.onInboxStatusChanged(record, oldStatus, newStatus, errorReason)
			} catch (t: Throwable) {
				log.warn("Fyke: FykeEventListener failed onInboxStatusChanged for id={}: {}", record.id, t.message)
			}
		}
	}

	fun notifyDlqCaptured(record: DlqRecord) {
		if (listeners.isEmpty()) return
		for (listener in listeners) {
			try {
				listener.onDlqCaptured(record)
			} catch (t: Throwable) {
				log.warn("Fyke: FykeEventListener failed onDlqCaptured for id={}: {}", record.id, t.message)
			}
		}
	}
}
