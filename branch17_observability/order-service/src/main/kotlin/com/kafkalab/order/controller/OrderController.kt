package com.kafkalab.order.controller

import com.kafkalab.order.model.OrderCreatedEvent
import com.kafkalab.order.service.OrderService
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/orders")
class OrderController(private val orderService: OrderService) {

    @PostMapping
    fun createOrder(
        @RequestParam userId: String,
        @RequestParam totalAmount: Double,
        @RequestParam category: String
    ): OrderCreatedEvent = orderService.send(userId, totalAmount, category)

    @PostMapping("/flood")
    fun flood(@RequestParam(defaultValue = "50") count: Int): Map<String, Any> =
        orderService.sendFlood(count)
}