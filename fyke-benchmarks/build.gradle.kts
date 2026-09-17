plugins {
	id("org.springframework.boot")
	kotlin("plugin.spring")
}

dependencies {
	implementation(project(":fyke-spring-boot-starter"))
	implementation("org.springframework.boot:spring-boot-starter")
	implementation("org.springframework.boot:spring-boot-starter-jdbc")
	implementation("org.springframework.boot:spring-boot-starter-amqp")
	implementation("org.springframework.boot:spring-boot-starter-kafka")
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	implementation("org.postgresql:postgresql")
	implementation("org.hdrhistogram:HdrHistogram:2.2.2")

	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.springframework.boot:spring-boot-testcontainers")
	testImplementation("org.testcontainers:postgresql")
	testImplementation("org.testcontainers:rabbitmq")
	testImplementation("org.testcontainers:kafka")
	testImplementation("org.testcontainers:junit-jupiter")
	testImplementation("org.awaitility:awaitility-kotlin")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.named<Test>("test") {
	useJUnitPlatform {
		excludeTags("benchmark")
	}
}

tasks.register<Test>("benchmark") {
	description = "Runs high-concurrency stress and latency benchmarks"
	group = "verification"
	testClassesDirs = sourceSets["test"].output.classesDirs
	classpath = sourceSets["test"].runtimeClasspath
	useJUnitPlatform {
		includeTags("benchmark")
	}
	maxHeapSize = "4096m"
	testLogging {
		showStandardStreams = true
		events("started", "passed", "failed", "skipped")
	}
}
