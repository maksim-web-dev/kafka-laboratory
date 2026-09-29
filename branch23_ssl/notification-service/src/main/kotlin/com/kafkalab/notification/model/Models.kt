package com.kafkalab.notification.model

data class OrderCreatedEvent(
    val orderId: String = "",
    val userId: String = "",
    val productId: String = "",
    val quantity: Int = 0,
    val totalAmount: Double = 0.0,
    val timestamp: String = ""
)

data class NotificationRecord(
    val orderId: String,
    val userId: String,
    val message: String,
    val receivedAt: String
)