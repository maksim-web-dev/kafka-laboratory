package com.kafkalab.notification.controller

import com.kafkalab.notification.listener.NotificationListener
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/notifications")
class NotificationController(private val listener: NotificationListener) {

    @GetMapping("/count")
    fun count(): Map<String, Any> {
        val created = listener.getCreatedCount()
        val cancelled = listener.getCancelledCount()
        val payments = listener.getPaymentsCount()
        return mapOf(
            "instanceId" to listener.getInstanceId(),
            "orders_created" to created,
            "orders_cancelled" to cancelled,
            "payments_processed" to payments,
            "total" to created + cancelled + payments
        )
    }

    @GetMapping("/config")
    fun config(): Map<String, Any> = mapOf(
        "instanceId" to listener.getInstanceId(),
        "listeners" to listOf("10.orders.created", "10.orders.cancelled", "10.payments.processed"),
        "typeMappings" to listOf(
            "OrderCreatedEvent → com.kafkalab.notification.model.OrderCreatedEvent",
            "OrderCancelledEvent → com.kafkalab.notification.model.OrderCancelledEvent",
            "PaymentProcessedEvent → com.kafkalab.notification.model.PaymentProcessedEvent"
        ),
        "trustedPackages" to "*"
    )
}