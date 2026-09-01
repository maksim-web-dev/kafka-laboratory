package com.kafkalab.notification.model

import java.time.LocalDateTime

data class PaymentProcessedEvent(
    val eventVersion: String = "1.0",
    val paymentId: String = "",
    val orderId: String = "",
    val userId: String = "",
    val amount: Double = 0.0,
    val status: PaymentStatus = PaymentStatus.APPROVED,
    val timestamp: LocalDateTime? = null
)