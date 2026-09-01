package com.kafkalab.order.model

data class CancelOrderRequest(
    val userId: String,
    val reason: String
)