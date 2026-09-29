package com.kafkalab.order.model

data class CreateOrderRequest(
    val userId: String,
    val productId: String,
    val quantity: Int = 1,
    val totalAmount: Double
)

data class OrderCreatedEvent(
    val orderId: String,
    val userId: String,
    val productId: String,
    val quantity: Int,
    val totalAmount: Double,
    val timestamp: String
)