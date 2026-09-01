package com.kafkalab.notification.model

import java.time.LocalDateTime

data class OrderCreatedEvent(
    val orderId: String = "",
    val userId: String = "",
    val product: String = "",
    val quantity: Int = 0,
    val totalAmount: Double = 0.0,
    val timestamp: LocalDateTime? = null
)