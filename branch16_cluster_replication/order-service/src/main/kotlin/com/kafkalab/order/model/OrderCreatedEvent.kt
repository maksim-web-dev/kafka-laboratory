package com.kafkalab.order.model

data class OrderCreatedEvent(
    val orderId: String,
    val userId: String,
    val totalAmount: Double,
    val category: String,
    val timestamp: Long = System.currentTimeMillis()
)