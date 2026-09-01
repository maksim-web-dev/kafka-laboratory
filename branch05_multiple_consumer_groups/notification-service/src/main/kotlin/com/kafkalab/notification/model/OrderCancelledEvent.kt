package com.kafkalab.notification.model

import java.time.LocalDateTime

data class OrderCancelledEvent(
    val orderId: String = "",
    val userId: String = "",
    val reason: String = "",
    val timestamp: LocalDateTime? = null
)