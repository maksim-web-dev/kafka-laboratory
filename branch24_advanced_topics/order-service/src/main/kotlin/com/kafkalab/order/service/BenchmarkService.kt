package com.kafkalab.order.service

import com.kafkalab.order.model.BenchmarkResult
import com.kafkalab.order.model.OrderCreatedEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.util.UUID
import kotlin.system.measureTimeMillis

@Service
class BenchmarkService(private val kafkaTemplate: KafkaTemplate<String, Any>) {

    private val log = LoggerFactory.getLogger(javaClass)
    private val products = listOf("prod-laptop", "prod-phone", "prod-tablet", "prod-headphones")
    private val users = listOf("user-1", "user-2", "user-3", "user-4", "user-5")

    fun runBenchmark(messageCount: Int): BenchmarkResult {
        log.info("[BENCHMARK] starting {} messages with lz4 compression", messageCount)
        val durationMs = measureTimeMillis {
            repeat(messageCount) { i ->
                val orderId = UUID.randomUUID().toString()
                val event = OrderCreatedEvent(
                    orderId = orderId,
                    userId = users[i % users.size],
                    productId = products[i % products.size],
                    quantity = 1,
                    totalAmount = (10..2000).random().toDouble(),
                    timestamp = LocalDateTime.now().toString()
                )
                kafkaTemplate.send("24.orders.benchmark", orderId, event)
            }
        }
        val throughput = if (durationMs > 0) messageCount * 1000.0 / durationMs else 0.0
        log.info("[BENCHMARK] done: {}ms, {:.1f} msg/sec", durationMs, throughput)
        return BenchmarkResult(messageCount, "lz4", durationMs, throughput)
    }
}