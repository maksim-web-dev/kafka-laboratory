package com.kafkalab.order.model

data class OrderSendResult(
    val orderId: String,
    val userId: String,
    val key: String?,
    val partition: Int,
    val offset: Long
)