import org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
	kotlin("jvm") version "2.3.21" apply false
	kotlin("plugin.spring") version "2.3.21" apply false
	id("org.springframework.boot") version "4.1.1" apply false
	id("io.spring.dependency-management") version "1.1.7"
	id("com.google.protobuf") version "0.9.4" apply false
	id("com.diffplug.spotless") version "8.10.2"
}


allprojects {
	group = "dev.fyke"
	version = "0.1.0-SNAPSHOT"

	apply(plugin = "com.diffplug.spotless")

	configure<com.diffplug.gradle.spotless.SpotlessExtension> {
		kotlin {
			target("src/**/*.kt")
			targetExclude("**/build/**")
			leadingSpacesToTabs(4)
			trimTrailingWhitespace()
			endWithNewline()
		}
		kotlinGradle {
			target("*.gradle.kts")
			targetExclude("**/build/**")
			leadingSpacesToTabs(4)
			trimTrailingWhitespace()
			endWithNewline()
		}
		format("yaml") {
			target("**/*.yml", "**/*.yaml")
			targetExclude("**/build/**", "**/.idea/**")
			leadingTabsToSpaces(2)
			trimTrailingWhitespace()
			endWithNewline()
		}
		format("xml") {
			target("**/*.xml")
			targetExclude("**/build/**", "**/.idea/**")
			leadingTabsToSpaces(4)
			trimTrailingWhitespace()
			endWithNewline()
		}
		format("sql") {
			target("**/*.sql")
			targetExclude("**/build/**", "**/.idea/**")
			leadingTabsToSpaces(4)
			trimTrailingWhitespace()
			endWithNewline()
		}
		format("markdown") {
			target("**/*.md")
			targetExclude("**/build/**", "**/.idea/**")
			leadingTabsToSpaces(2)
			endWithNewline()
		}
		format("misc") {
			target("**/.gitignore", "**/.editorconfig", "**/*.properties")
			targetExclude("**/build/**", "**/.idea/**")
			trimTrailingWhitespace()
			endWithNewline()
		}
	}
}


subprojects {
	apply(plugin = "org.jetbrains.kotlin.jvm")
	apply(plugin = "io.spring.dependency-management")

	repositories {
		mavenCentral()
	}

	dependencyManagement {
		imports {
			mavenBom("org.springframework.boot:spring-boot-dependencies:4.1.1")
			mavenBom("org.testcontainers:testcontainers-bom:1.20.5")
		}
		dependencies {
			dependency("io.mockk:mockk:1.13.17")
		}
	}

	configure<KotlinJvmProjectExtension> {
		jvmToolchain(21)
	}

	tasks.withType<KotlinCompile>().configureEach {
		compilerOptions {
			freeCompilerArgs.addAll(
				"-Xjsr305=strict",
				"-Xannotation-default-target=param-property"
			)
		}
	}

	tasks.withType<Test> {
		useJUnitPlatform()
		maxHeapSize = "2048m"
		val dockerSock = File("${System.getProperty("user.home")}/.docker/run/docker.sock")
		if (dockerSock.exists() && System.getenv("DOCKER_HOST") == null) {
			environment("DOCKER_HOST", "unix://${dockerSock.absolutePath}")
		}
		systemProperty("api.version", "1.44")
	}
}
