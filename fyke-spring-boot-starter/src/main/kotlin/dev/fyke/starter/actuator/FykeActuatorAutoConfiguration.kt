package dev.fyke.starter.actuator

import dev.fyke.core.binder.BrokerBinder
import dev.fyke.core.inbox.InboxPollerEngine
import dev.fyke.core.inbox.InboxStore
import dev.fyke.core.outbox.OutboxPollerEngine
import dev.fyke.core.outbox.OutboxStore
import dev.fyke.starter.FykeAutoConfiguration
import dev.fyke.starter.properties.FykeProperties
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint
import org.springframework.boot.actuate.endpoint.annotation.Endpoint
import org.springframework.boot.health.autoconfigure.contributor.ConditionalOnEnabledHealthIndicator
import org.springframework.boot.health.contributor.HealthIndicator
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * Auto-configuration for Fyke Actuator health indicator, diagnostic endpoint, and Micrometer metrics.
 */
@AutoConfiguration(after = [FykeAutoConfiguration::class])
class FykeActuatorAutoConfiguration {

	@Configuration(proxyBeanMethods = false)
	@ConditionalOnClass(HealthIndicator::class)
	@ConditionalOnEnabledHealthIndicator("fyke")
	class HealthConfiguration {
		@Bean
		@ConditionalOnMissingBean(name = ["fykeHealthIndicator"])
		fun fykeHealthIndicator(
			outboxStoreProvider: ObjectProvider<OutboxStore>,
			inboxStoreProvider: ObjectProvider<InboxStore>,
			outboxPollerEngineProvider: ObjectProvider<OutboxPollerEngine>,
			inboxPollerEngineProvider: ObjectProvider<InboxPollerEngine>,
			properties: FykeProperties
		): FykeHealthIndicator {
			return FykeHealthIndicator(
				outboxStoreProvider = outboxStoreProvider,
				inboxStoreProvider = inboxStoreProvider,
				outboxPollerEngineProvider = outboxPollerEngineProvider,
				inboxPollerEngineProvider = inboxPollerEngineProvider,
				properties = properties
			)
		}
	}

	@Configuration(proxyBeanMethods = false)
	@ConditionalOnClass(Endpoint::class)
	@ConditionalOnAvailableEndpoint(endpoint = FykeEndpoint::class)
	class EndpointConfiguration {
		@Bean
		@ConditionalOnMissingBean(name = ["fykeEndpoint"])
		fun fykeEndpoint(
			outboxStoreProvider: ObjectProvider<OutboxStore>,
			inboxStoreProvider: ObjectProvider<InboxStore>,
			outboxPollerEngineProvider: ObjectProvider<OutboxPollerEngine>,
			inboxPollerEngineProvider: ObjectProvider<InboxPollerEngine>,
			brokerBinderProvider: ObjectProvider<BrokerBinder>,
			properties: FykeProperties
		): FykeEndpoint {
			return FykeEndpoint(
				outboxStoreProvider = outboxStoreProvider,
				inboxStoreProvider = inboxStoreProvider,
				outboxPollerEngineProvider = outboxPollerEngineProvider,
				inboxPollerEngineProvider = inboxPollerEngineProvider,
				brokerBinderProvider = brokerBinderProvider,
				properties = properties
			)
		}
	}

	@Configuration(proxyBeanMethods = false)
	@ConditionalOnClass(MeterRegistry::class)
	class MetricsConfiguration {
		@Bean
		@ConditionalOnMissingBean
		fun fykeMeterBinder(
			outboxStoreProvider: ObjectProvider<OutboxStore>,
			inboxStoreProvider: ObjectProvider<InboxStore>
		): FykeMeterBinder {
			return FykeMeterBinder(
				outboxStoreProvider = outboxStoreProvider,
				inboxStoreProvider = inboxStoreProvider
			)
		}
	}
}
