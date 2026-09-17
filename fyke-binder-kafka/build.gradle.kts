plugins {
	`java-library`
}

dependencies {
	api(project(":fyke-core"))
	api("org.springframework.kafka:spring-kafka")
	api("org.slf4j:slf4j-api")

	testImplementation("org.junit.jupiter:junit-jupiter")
	testImplementation("org.assertj:assertj-core")
	testImplementation("org.awaitility:awaitility")
	testImplementation("io.mockk:mockk")
	testImplementation("org.testcontainers:testcontainers")
	testImplementation("org.testcontainers:kafka")
	testImplementation("org.testcontainers:junit-jupiter")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
	testRuntimeOnly("ch.qos.logback:logback-classic:1.5.16")
}
