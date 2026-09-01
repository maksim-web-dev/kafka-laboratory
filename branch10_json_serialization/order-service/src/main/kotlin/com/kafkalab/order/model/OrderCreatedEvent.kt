package com.kafkalab.order.model

import java.time.LocalDateTime
import java.util.UUID

data class OrderCreatedEvent(
    val eventVersion: String = "1.0",
    val orderId: String = UUID.randomUUID().toString(),
    val userId: String,
    val items: List<OrderItem> = emptyList(),
    val totalAmount: Double,
    val timestamp: LocalDateTime = LocalDateTime.now()
)