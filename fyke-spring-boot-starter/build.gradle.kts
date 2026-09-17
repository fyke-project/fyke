plugins {
	`java-library`
}

dependencies {
	api(project(":fyke-core"))
	api(project(":fyke-binder-rabbitmq"))
	api(project(":fyke-binder-kafka"))

	api("org.springframework.boot:spring-boot-starter")
	api("org.springframework.boot:spring-boot-starter-jdbc")

	compileOnly("org.springframework.boot:spring-boot-configuration-processor")
	compileOnly("org.springframework.boot:spring-boot-actuator")
	compileOnly("org.springframework.boot:spring-boot-actuator-autoconfigure")
	compileOnly("org.springframework.boot:spring-boot-health")
	compileOnly("io.micrometer:micrometer-core")

	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.springframework.boot:spring-boot-actuator")
	testImplementation("org.springframework.boot:spring-boot-actuator-autoconfigure")
	testImplementation("org.springframework.boot:spring-boot-health")
	testImplementation("io.micrometer:micrometer-core")
	testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
	testImplementation("io.mockk:mockk")
	testImplementation("org.assertj:assertj-core")
	testImplementation("org.testcontainers:postgresql")
	testImplementation("org.testcontainers:rabbitmq")
	testImplementation("org.testcontainers:junit-jupiter")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
