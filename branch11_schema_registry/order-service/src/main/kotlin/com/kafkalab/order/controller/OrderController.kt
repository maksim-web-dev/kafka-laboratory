package com.kafkalab.order.controller

import com.kafkalab.order.service.OrderService
import org.springframework.web.bind.annotation.*

data class CreateOrderRequest(val userId: String, val itemCount: Int = 2)
data class CancelOrderRequest(val userId: String, val reason: String)

@RestController
@RequestMapping("/api/orders")
class OrderController(private val orderService: OrderService) {

    @PostMapping
    fun createOrder(@RequestBody req: CreateOrderRequest): Map<String, String> {
        val orderId = orderService.createOrder(req.userId, req.itemCount)
        return mapOf("orderId" to orderId, "status" to "CREATED")
    }

    @PostMapping("/{orderId}/cancel")
    fun cancelOrder(
        @PathVariable orderId: String,
        @RequestBody req: CancelOrderRequest
    ): Map<String, String> {
        orderService.cancelOrder(orderId, req.userId, req.reason)
        return mapOf("orderId" to orderId, "status" to "CANCELLED")
    }
}