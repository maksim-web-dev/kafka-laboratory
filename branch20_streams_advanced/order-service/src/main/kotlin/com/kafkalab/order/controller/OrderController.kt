package com.kafkalab.order.controller

import com.kafkalab.order.model.CreateOrderRequest
import com.kafkalab.order.service.OrderService
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/orders")
class OrderController(private val orderService: OrderService) {

    @PostMapping
    fun createOrder(@RequestBody req: CreateOrderRequest) = orderService.createOrder(req)

    @PostMapping("/batch")
    fun createBatch(
        @RequestParam users: String,
        @RequestParam(defaultValue = "10") count: Int
    ) = mapOf(
        "orderIds" to orderService.createBatch(users.split(","), count),
        "count" to count
    )
}