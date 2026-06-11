package com.kafkalab.notification.controller

import com.kafkalab.notification.listener.OrderEventListener
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/notifications")
class NotificationController(private val listener: OrderEventListener) {

    @GetMapping("/count")
    fun count(): Map<String, Int> = mapOf("received" to listener.getReceivedCount())
}