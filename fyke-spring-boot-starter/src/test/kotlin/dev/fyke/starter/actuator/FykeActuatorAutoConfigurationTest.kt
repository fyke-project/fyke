package dev.fyke.starter.actuator

import dev.fyke.starter.FykeAutoConfiguration
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.FilteredClassLoader
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.sql.DataSource

import org.springframework.boot.actuate.autoconfigure.endpoint.EndpointAutoConfiguration

class FykeActuatorAutoConfigurationTest {

	private val contextRunner = ApplicationContextRunner()
		.withConfiguration(
			AutoConfigurations.of(
				FykeAutoConfiguration::class.java,
				FykeActuatorAutoConfiguration::class.java,
				EndpointAutoConfiguration::class.java
			)
		)
		.withUserConfiguration(MockDependenciesConfig::class.java)
		.withPropertyValues(
			"fyke.liquibase.enabled=false",
			"management.endpoints.web.exposure.include=*",
			"management.endpoint.fyke.access=UNRESTRICTED"
		)

	@Test
	fun `should auto-configure health indicator, endpoint, and meter binder`() {
		contextRunner.run { context ->
			assertThat(context).hasSingleBean(FykeHealthIndicator::class.java)
			assertThat(context).hasSingleBean(FykeEndpoint::class.java)
			assertThat(context).hasSingleBean(FykeMeterBinder::class.java)
		}
	}

	@Test
	fun `should back off when HealthIndicator class is not on classpath`() {
		contextRunner
			.withClassLoader(FilteredClassLoader(org.springframework.boot.health.contributor.HealthIndicator::class.java))
			.run { context ->
				assertThat(context).doesNotHaveBean(FykeHealthIndicator::class.java)
				assertThat(context).hasSingleBean(FykeEndpoint::class.java)
				assertThat(context).hasSingleBean(FykeMeterBinder::class.java)
			}
	}

	@Test
	fun `should back off when MeterRegistry class is not on classpath`() {
		contextRunner
			.withClassLoader(FilteredClassLoader(MeterRegistry::class.java))
			.run { context ->
				assertThat(context).hasSingleBean(FykeHealthIndicator::class.java)
				assertThat(context).hasSingleBean(FykeEndpoint::class.java)
				assertThat(context).doesNotHaveBean(FykeMeterBinder::class.java)
			}
	}

	@Configuration(proxyBeanMethods = false)
	open class MockDependenciesConfig {
		@Bean
		open fun dataSource(): DataSource = mockk<DataSource>(relaxed = true)

		@Bean
		open fun rabbitTemplate(): RabbitTemplate = mockk<RabbitTemplate>(relaxed = true)

		@Bean
		open fun meterRegistry(): MeterRegistry = SimpleMeterRegistry()
	}
}
