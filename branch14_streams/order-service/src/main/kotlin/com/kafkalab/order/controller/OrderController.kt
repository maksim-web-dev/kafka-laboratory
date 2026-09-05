package com.kafkalab.order.controller

import com.kafkalab.order.service.OrderService
import org.springframework.web.bind.annotation.*

data class CreateOrderRequest(
    val userId: String,
    val itemCount: Int = 2,
    val category: String? = null
)

@RestController
@RequestMapping("/api/orders")
class OrderController(private val orderService: OrderService) {

    @PostMapping
    fun createOrder(@RequestBody req: CreateOrderRequest): Map<String, String> {
        val orderId = orderService.createOrder(req.userId, req.itemCount, req.category)
        return mapOf("orderId" to orderId, "status" to "CREATED")
    }

    @PostMapping("/batch")
    fun createBatch(
        @RequestParam users: List<String>,
        @RequestParam(defaultValue = "5") count: Int
    ): Map<String, Any> {
        val categories = listOf("ELECTRONICS", "FOOD", "CLOTHING", "GENERAL")
        var total = 0
        repeat(count) {
            val userId = users.random()
            val category = categories.random()
            orderService.createOrder(userId, itemCount = (1..4).random(), category = category)
            total++
        }
        return mapOf("created" to total, "users" to users)
    }
}