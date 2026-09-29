package com.kafkalab.analytics.model

data class OrderCreatedEvent(
    val orderId: String,
    val userId: String,
    val productId: String,
    val quantity: Int,
    val totalAmount: Double,
    val timestamp: String
)

data class PaymentProcessedEvent(
    val paymentId: String,
    val orderId: String,
    val userId: String,
    val amount: Double,
    val status: String,
    val timestamp: String
)

data class UserProfile(
    val userId: String,
    val name: String,
    val email: String,
    val tier: String,
    val country: String
)

data class ProductEvent(
    val productId: String,
    val name: String,
    val category: String,
    val price: Double
)

data class EnrichedOrder(
    val orderId: String,
    val userId: String,
    val userName: String,
    val userTier: String,
    val productId: String,
    val productName: String,
    val productCategory: String,
    val totalAmount: Double,
    val timestamp: String
)

data class MatchedOrder(
    val orderId: String,
    val userId: String,
    val totalAmount: Double,
    val paymentStatus: String,
    val orderTimestamp: String,
    val paymentTimestamp: String
)