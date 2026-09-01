package com.kafkalab.order.service

import com.kafkalab.order.model.BenchmarkResult
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class BenchmarkService(
    @Value("\${spring.kafka.bootstrap-servers}") bootstrapServers: String
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val benchmarkTopic = "07.benchmark"

    private val acks0Template = buildTemplate(bootstrapServers, acks = "0", lingerMs = 0, batchSize = 16384, retries = 0, idempotent = false)
    private val acks1Template = buildTemplate(bootstrapServers, acks = "1", lingerMs = 5, batchSize = 32768, retries = 3, idempotent = false)
    private val acksAllTemplate = buildTemplate(bootstrapServers, acks = "all", lingerMs = 20, batchSize = 65536, retries = Int.MAX_VALUE, idempotent = true)

    fun runBenchmark(mode: String, count: Int): BenchmarkResult {
        val template: KafkaTemplate<String, String>
        val acksConfig: String
        val lingerMs: Int
        val batchSize: Int
        val idempotent: Boolean

        when (mode) {
            "acks0" -> { template = acks0Template; acksConfig = "0"; lingerMs = 0; batchSize = 16384; idempotent = false }
            "acks1" -> { template = acks1Template; acksConfig = "1"; lingerMs = 5; batchSize = 32768; idempotent = false }
            "acks-all" -> { template = acksAllTemplate; acksConfig = "all"; lingerMs = 20; batchSize = 65536; idempotent = true }
            else -> throw IllegalArgumentException("Unknown mode: $mode. Use: acks0, acks1, acks-all")
        }

        log.info("Starting benchmark: mode={}, count={}", mode, count)
        val start = System.currentTimeMillis()

        repeat(count) { i ->
            template.send(benchmarkTopic, "key-$i", "benchmark-message-$i").get()
        }

        val duration = System.currentTimeMillis() - start
        val throughput = if (duration > 0) count * 1000.0 / duration else count * 1000.0
        val avgLatency = duration.toDouble() / count

        log.info("Benchmark done: mode={}, count={}, durationMs={}, throughput={} msg/s",
            mode, count, duration, "%.1f".format(throughput))

        return BenchmarkResult(
            mode = mode,
            acksConfig = acksConfig,
            idempotent = idempotent,
            lingerMs = lingerMs,
            batchSizeBytes = batchSize,
            messageCount = count,
            totalDurationMs = duration,
            throughputMsgPerSec = throughput,
            avgLatencyMs = avgLatency
        )
    }

    fun compareAll(count: Int): Map<String, BenchmarkResult> = mapOf(
        "acks0" to runBenchmark("acks0", count),
        "acks1" to runBenchmark("acks1", count),
        "acks-all" to runBenchmark("acks-all", count)
    )
}

private fun buildTemplate(
    bootstrapServers: String,
    acks: String,
    lingerMs: Int,
    batchSize: Int,
    retries: Int,
    idempotent: Boolean
): KafkaTemplate<String, String> {
    val props = mutableMapOf<String, Any>(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.ACKS_CONFIG to acks,
        ProducerConfig.LINGER_MS_CONFIG to lingerMs,
        ProducerConfig.BATCH_SIZE_CONFIG to batchSize,
        ProducerConfig.RETRIES_CONFIG to retries,
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to idempotent
    )
    if (idempotent) {
        props[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION] = 5
    }
    return KafkaTemplate(DefaultKafkaProducerFactory(props))
}