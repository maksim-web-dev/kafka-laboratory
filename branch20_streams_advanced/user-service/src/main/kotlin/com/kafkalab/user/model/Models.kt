package com.kafkalab.user.model

data class UserProfile(
    val userId: String,
    val name: String,
    val email: String,
    val tier: String = "STANDARD",
    val country: String = "UA"
)

data class UpsertUserRequest(
    val name: String,
    val email: String,
    val tier: String = "STANDARD",
    val country: String = "UA"
)

data class ProductEvent(
    val productId: String,
    val name: String,
    val category: String,
    val price: Double
)