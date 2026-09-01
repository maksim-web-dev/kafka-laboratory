package com.kafkalab.order.model

import java.time.LocalDateTime
import java.util.UUID

data class OrderCreatedEvent(
    val orderId: String = UUID.randomUUID().toString(),
    val userId: String,
    val product: String,
    val quantity: Int,
    val totalAmount: Double,
    val timestamp: LocalDateTime = LocalDateTime.now()
)