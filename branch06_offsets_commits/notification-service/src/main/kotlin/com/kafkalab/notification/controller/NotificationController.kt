package com.kafkalab.notification.controller

import com.kafkalab.notification.listener.OrderEventListener
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/notifications")
class NotificationController(private val listener: OrderEventListener) {

    @GetMapping("/count")
    fun count(): Map<String, Any> {
        val created = listener.getCreatedCount()
        val cancelled = listener.getCancelledCount()
        val redelivered = listener.getRedeliveredCount()
        return mapOf(
            "instanceId" to listener.getInstanceId(),
            "orders_created" to created,
            "orders_cancelled" to cancelled,
            "redelivered" to redelivered,
            "total" to created + cancelled
        )
    }

    @PostMapping("/simulate-failure/{count}")
    fun simulateFailure(@PathVariable count: Int): Map<String, Any> {
        listener.setFailNextN(count)
        return mapOf(
            "instanceId" to listener.getInstanceId(),
            "scheduledNacks" to count,
            "message" to "Next $count message(s) will be nacked and redelivered after 2s"
        )
    }
}