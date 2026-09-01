package com.kafkalab.order.model

data class BatchOrderRequest(
    val userId: String,
    val count: Int,
    val product: String = "Kafka Book"
)