package com.kafkalab.payment.model

import java.time.LocalDateTime

data class OrderCreatedEvent(
    val eventVersion: String = "1.0",
    val orderId: String = "",
    val userId: String = "",
    val items: List<OrderItem> = emptyList(),
    val totalAmount: Double = 0.0,
    val timestamp: LocalDateTime? = null
)