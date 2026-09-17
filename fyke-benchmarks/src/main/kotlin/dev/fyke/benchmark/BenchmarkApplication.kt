package dev.fyke.benchmark

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.transaction.annotation.EnableTransactionManagement

@SpringBootApplication(scanBasePackages = ["dev.fyke"])
@EnableTransactionManagement
class BenchmarkApplication
