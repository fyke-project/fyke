package dev.fyke.core.telemetry

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class FykeTelemetryTest {

	private lateinit var spanExporter: InMemorySpanExporter
	private lateinit var openTelemetry: OpenTelemetry
	private lateinit var telemetry: FykeTelemetry

	@BeforeEach
	fun setUp() {
		spanExporter = InMemorySpanExporter.create()
		val tracerProvider = SdkTracerProvider.builder()
			.addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
			.build()

		openTelemetry = OpenTelemetrySdk.builder()
			.setTracerProvider(tracerProvider)
			.build()

		telemetry = FykeTelemetry(openTelemetry)
	}

	@AfterEach
	fun tearDown() {
		spanExporter.reset()
	}

	@Test
	fun `recordSpan should create span, activate context, and record attributes`() {
		var capturedTraceId: String? = null

		val result = telemetry.recordSpan("test.operation", mapOf("business_key" to "biz-123")) { span ->
			assertThat(span.spanContext.isValid).isTrue()
			capturedTraceId = telemetry.currentTraceId()
			assertThat(capturedTraceId).isEqualTo(span.spanContext.traceId)
			"success"
		}

		assertThat(result).isEqualTo("success")
		assertThat(telemetry.currentTraceId()).isNull()

		val finishedSpans = spanExporter.finishedSpanItems
		assertThat(finishedSpans).hasSize(1)
		val finished = finishedSpans.first()
		assertThat(finished.name).isEqualTo("test.operation")
		assertThat(finished.spanContext.traceId).isEqualTo(capturedTraceId)
		assertThat(finished.attributes.asMap().keys.map { it.key }).contains("business_key")
	}

	@Test
	fun `recordSpan should inherit parent trace context if active`() {
		val parentTracer = openTelemetry.getTracer("test")
		val parentSpan = parentTracer.spanBuilder("parent.request").startSpan()
		val parentScope = parentSpan.makeCurrent()

		try {
			val parentTraceId = parentSpan.spanContext.traceId
			assertThat(telemetry.currentTraceId()).isEqualTo(parentTraceId)

			telemetry.recordSpan("child.operation") { childSpan ->
				assertThat(childSpan.spanContext.traceId).isEqualTo(parentTraceId)
				assertThat(telemetry.currentTraceId()).isEqualTo(parentTraceId)
			}
		} finally {
			parentScope.close()
			parentSpan.end()
		}

		val spans = spanExporter.finishedSpanItems
		assertThat(spans).hasSize(2)
		val child = spans.first { it.name == "child.operation" }
		val parent = spans.first { it.name == "parent.request" }
		assertThat(child.parentSpanId).isEqualTo(parent.spanId)
		assertThat(child.traceId).isEqualTo(parent.traceId)
	}

	@Test
	fun `currentTraceId returns null when no span is active and fallback is noop`() {
		val noopTelemetry = FykeTelemetry(OpenTelemetry.noop())
		assertThat(noopTelemetry.currentTraceId()).isNull()

		noopTelemetry.recordSpan("noop.span") {
			assertThat(noopTelemetry.currentTraceId()).isNull()
		}
	}
}
