plugins {
	`java-library`
}

dependencies {
	api("org.springframework:spring-context")
	api("org.springframework:spring-jdbc")
	api("org.springframework:spring-tx")
	api("com.fasterxml.jackson.module:jackson-module-kotlin")
	api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

	implementation("org.postgresql:postgresql")
	api("org.liquibase:liquibase-core")
	implementation("org.slf4j:slf4j-api")

	api("io.opentelemetry:opentelemetry-api")

	testImplementation("org.junit.jupiter:junit-jupiter")
	testImplementation("org.assertj:assertj-core")
	testImplementation("io.mockk:mockk")
	testImplementation("org.testcontainers:testcontainers")
	testImplementation("org.testcontainers:postgresql")
	testImplementation("org.testcontainers:junit-jupiter")
	testImplementation("com.zaxxer:HikariCP")
	testImplementation("io.opentelemetry:opentelemetry-sdk")
	testImplementation("io.opentelemetry:opentelemetry-sdk-testing")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
	testRuntimeOnly("ch.qos.logback:logback-classic:1.5.16")
}
