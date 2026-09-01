package com.kafkalab.order.model

import java.util.UUID

data class OrderItem(
    val productId: String = UUID.randomUUID().toString(),
    val productName: String = "",
    val quantity: Int = 0,
    val unitPrice: Double = 0.0
)