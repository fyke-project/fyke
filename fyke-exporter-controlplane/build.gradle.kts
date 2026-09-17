import com.google.protobuf.gradle.id

plugins {
	kotlin("jvm")
	kotlin("plugin.spring")
	id("com.google.protobuf")
}

val grpcVersion = "1.83.1"
val protobufVersion = "3.25.5"

dependencies {
	implementation(project(":fyke-core"))
	implementation(project(":fyke-spring-boot-starter"))

	implementation("org.springframework.boot:spring-boot-starter")
	implementation("org.springframework.boot:spring-boot-starter-validation")

	api("io.grpc:grpc-protobuf:$grpcVersion")
	api("io.grpc:grpc-stub:$grpcVersion")
	implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
	api("com.google.protobuf:protobuf-java:$protobufVersion")

	compileOnly("jakarta.annotation:jakarta.annotation-api:3.0.0")
	compileOnly("javax.annotation:javax.annotation-api:1.3.2")

	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
	testImplementation("io.mockk:mockk:1.13.17")
	testImplementation("io.grpc:grpc-testing:$grpcVersion")
	testImplementation("io.grpc:grpc-inprocess:$grpcVersion")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

protobuf {
	protoc {
		artifact = "com.google.protobuf:protoc:$protobufVersion"
	}
	plugins {
		id("grpc") {
			artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
		}
	}
	generateProtoTasks {
		ofSourceSet("main").forEach { task ->
			task.plugins {
				id("grpc")
			}
		}
	}
}
