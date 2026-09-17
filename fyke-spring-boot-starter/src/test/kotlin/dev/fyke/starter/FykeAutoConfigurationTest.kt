package dev.fyke.starter

import dev.fyke.binder.kafka.KafkaBinder
import dev.fyke.binder.rabbit.RabbitBinder
import dev.fyke.core.binder.BrokerBinder
import dev.fyke.core.outbox.OutboxPollerEngine
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.FilteredClassLoader
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaTemplate
import javax.sql.DataSource
import io.mockk.mockk

class FykeAutoConfigurationTest {

	private val contextRunner = ApplicationContextRunner()
		.withConfiguration(AutoConfigurations.of(FykeAutoConfiguration::class.java))
		.withPropertyValues(
			"fyke.liquibase.enabled=false"
		)

	@Test
	fun `should configure RabbitBinder when RabbitTemplate is present`() {
		contextRunner
			.withUserConfiguration(MockDataSourceConfig::class.java, MockRabbitConfig::class.java)
			.run { context ->
				assertThat(context).hasSingleBean(BrokerBinder::class.java)
				assertThat(context.getBean(BrokerBinder::class.java)).isInstanceOf(RabbitBinder::class.java)
				assertThat(context).hasSingleBean(OutboxPollerEngine::class.java)
			}
	}

	@Test
	fun `should configure KafkaBinder when KafkaTemplate is present`() {
		contextRunner
			.withUserConfiguration(MockDataSourceConfig::class.java, MockKafkaConfig::class.java)
			.run { context ->
				assertThat(context).hasSingleBean(BrokerBinder::class.java)
				assertThat(context.getBean(BrokerBinder::class.java)).isInstanceOf(KafkaBinder::class.java)
				assertThat(context).hasSingleBean(OutboxPollerEngine::class.java)
			}
	}

	@Test
	fun `should select KafkaBinder when both templates present and fyke binder property set to kafka`() {
		contextRunner
			.withUserConfiguration(MockDataSourceConfig::class.java, MockRabbitConfig::class.java, MockKafkaConfig::class.java)
			.withPropertyValues("fyke.binder=kafka")
			.run { context ->
				val poller = context.getBean(OutboxPollerEngine::class.java)
				assertThat(poller).isNotNull
			}
	}

	@Configuration(proxyBeanMethods = false)
	open class MockDataSourceConfig {
		@Bean
		open fun dataSource(): DataSource = mockk<DataSource>(relaxed = true)
	}

	@Configuration(proxyBeanMethods = false)
	open class MockRabbitConfig {
		@Bean
		open fun rabbitTemplate(): RabbitTemplate = mockk<RabbitTemplate>(relaxed = true)
	}

	@Configuration(proxyBeanMethods = false)
	open class MockKafkaConfig {
		@Bean
		open fun kafkaTemplate(): KafkaTemplate<*, *> = mockk<KafkaTemplate<*, *>>(relaxed = true)
	}
}
