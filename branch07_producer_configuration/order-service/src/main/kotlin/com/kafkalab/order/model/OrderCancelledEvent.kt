package com.kafkalab.order.model

import java.time.LocalDateTime

data class OrderCancelledEvent(
    val orderId: String,
    val userId: String,
    val reason: String,
    val timestamp: LocalDateTime = LocalDateTime.now()
)