package com.kafkalab.order.model

data class CreateOrderRequest(
    val userId: String,
    val product: String,
    val quantity: Int,
    val totalAmount: Double
)