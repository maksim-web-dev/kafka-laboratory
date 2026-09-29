package com.kafkalab.order.controller

import com.kafkalab.order.interceptor.MetricsProducerInterceptor
import com.kafkalab.order.model.CreateOrderRequest
import com.kafkalab.order.service.BenchmarkService
import com.kafkalab.order.service.OrderService
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/orders")
class OrderController(
    private val orderService: OrderService,
    private val benchmarkService: BenchmarkService
) {

    @PostMapping
    fun createOrder(@RequestBody req: CreateOrderRequest) = orderService.createOrder(req)

    @PostMapping("/benchmark")
    fun runBenchmark(@RequestParam(defaultValue = "1000") count: Int) =
        benchmarkService.runBenchmark(count)

    @GetMapping("/metrics")
    fun getMetrics() = mapOf(
        "interceptor" to "MetricsProducerInterceptor",
        "sentCount" to MetricsProducerInterceptor.sentCount.get(),
        "ackCount" to MetricsProducerInterceptor.ackCount.get(),
        "errorCount" to MetricsProducerInterceptor.errorCount.get(),
        "perTopic" to MetricsProducerInterceptor.topicCounts.mapValues { it.value.get() }
    )

    @GetMapping("/compression-info")
    fun compressionInfo() = mapOf(
        "current" to "lz4",
        "comparison" to mapOf(
            "none" to "No compression, lowest CPU, highest network/disk usage",
            "gzip" to "Best ratio, highest CPU cost — good for cold storage",
            "lz4" to "Best speed/ratio balance — recommended for most streaming workloads",
            "snappy" to "Good speed, moderate ratio — common in Hadoop ecosystems",
            "zstd" to "Better ratio than lz4 at similar speed — best for large messages"
        ),
        "broker_config" to "compression.type=producer means broker keeps whatever compression the producer used"
    )
}