package com.kafkalab.order.controller

import com.kafkalab.order.service.OrderSagaService
import org.springframework.web.bind.annotation.*

data class CreateOrderRequest(val userId: String, val itemCount: Int = 2)

@RestController
@RequestMapping("/api/orders")
class OrderController(private val sagaService: OrderSagaService) {

    @PostMapping
    fun createOrder(@RequestBody req: CreateOrderRequest): Map<String, String> {
        val orderId = sagaService.createOrder(req.userId, req.itemCount)
        return mapOf("orderId" to orderId, "status" to "PENDING")
    }

    @GetMapping("/{orderId}/status")
    fun getStatus(@PathVariable orderId: String): Map<String, String> =
        mapOf("orderId" to orderId, "status" to sagaService.getStatus(orderId))
}