plugins {
	`java-library`
}

dependencies {
	api(project(":fyke-core"))
	api(project(":fyke-binder-rabbitmq"))

	api("org.springframework.boot:spring-boot-starter")
	api("org.springframework.boot:spring-boot-starter-jdbc")

	compileOnly("org.springframework.boot:spring-boot-configuration-processor")

	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
	testImplementation("io.mockk:mockk")
	testImplementation("org.assertj:assertj-core")
	testImplementation("org.testcontainers:postgresql")
	testImplementation("org.testcontainers:rabbitmq")
	testImplementation("org.testcontainers:junit-jupiter")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
