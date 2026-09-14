plugins {
	`java-library`
}

dependencies {
	api(project(":fyke-core"))
	api("org.springframework.amqp:spring-rabbit")

	testImplementation("org.junit.jupiter:junit-jupiter")
	testImplementation("org.assertj:assertj-core")
	testImplementation("io.mockk:mockk")
	testImplementation("org.testcontainers:testcontainers")
	testImplementation("org.testcontainers:rabbitmq")
	testImplementation("org.testcontainers:junit-jupiter")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
	testRuntimeOnly("ch.qos.logback:logback-classic:1.5.16")
}
