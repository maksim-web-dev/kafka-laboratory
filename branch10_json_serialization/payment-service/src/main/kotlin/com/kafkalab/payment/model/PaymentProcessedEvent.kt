package com.kafkalab.payment.model

import java.time.LocalDateTime
import java.util.UUID

data class PaymentProcessedEvent(
    val eventVersion: String = "1.0",
    val paymentId: String = UUID.randomUUID().toString(),
    val orderId: String,
    val userId: String,
    val amount: Double,
    val status: PaymentStatus,
    val timestamp: LocalDateTime = LocalDateTime.now()
)