package com.kafkalab.notification.controller

import com.kafkalab.notification.listener.OrderEventListener
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/notifications")
class NotificationController(private val listener: OrderEventListener) {

    @GetMapping("/count")
    fun count(): Map<String, Any> {
        val created = listener.getCreatedCount()
        val cancelled = listener.getCancelledCount()
        return mapOf(
            "instanceId" to listener.getInstanceId(),
            "orders_created" to created,
            "orders_cancelled" to cancelled,
            "total" to created + cancelled
        )
    }
}