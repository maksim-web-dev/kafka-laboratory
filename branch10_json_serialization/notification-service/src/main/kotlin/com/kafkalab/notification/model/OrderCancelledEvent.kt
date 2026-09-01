package com.kafkalab.notification.model

import java.time.LocalDateTime

data class OrderCancelledEvent(
    val eventVersion: String = "1.0",
    val orderId: String = "",
    val userId: String = "",
    val reason: String = "",
    val timestamp: LocalDateTime? = null
)