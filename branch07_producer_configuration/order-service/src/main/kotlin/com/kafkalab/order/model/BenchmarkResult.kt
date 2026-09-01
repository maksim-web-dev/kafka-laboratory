package com.kafkalab.order.model

data class BenchmarkResult(
    val mode: String,
    val acksConfig: String,
    val idempotent: Boolean,
    val lingerMs: Int,
    val batchSizeBytes: Int,
    val messageCount: Int,
    val totalDurationMs: Long,
    val throughputMsgPerSec: Double,
    val avgLatencyMs: Double
)