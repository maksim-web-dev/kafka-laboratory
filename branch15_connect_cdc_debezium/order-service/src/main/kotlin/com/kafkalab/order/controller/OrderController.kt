package com.kafkalab.order.controller

import com.kafkalab.order.entity.Order
import com.kafkalab.order.service.OrderService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

data class CreateOrderRequest(
    val userId: String,
    val totalAmount: Double,
    val category: String = "GENERAL"
)

@RestController
@RequestMapping("/api/orders")
class OrderController(private val orderService: OrderService) {

    @PostMapping
    fun createOrder(@RequestBody req: CreateOrderRequest): Order =
        orderService.create(req.userId, req.totalAmount, req.category)

    @PostMapping("/batch")
    fun createBatch(
        @RequestParam users: List<String>,
        @RequestParam(defaultValue = "5") count: Int
    ): Map<String, Any> = orderService.createBatch(users, count)

    @GetMapping
    fun listOrders(): List<Order> = orderService.listAll()
}