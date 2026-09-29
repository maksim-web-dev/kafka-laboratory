package com.kafkalab.notification.controller

import com.kafkalab.notification.listener.OrderEventListener
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/notifications")
class NotificationController(private val listener: OrderEventListener) {

    @GetMapping
    fun getAll() = listener.notifications

    @GetMapping("/count")
    fun getCount() = mapOf("count" to listener.notifications.size)
}