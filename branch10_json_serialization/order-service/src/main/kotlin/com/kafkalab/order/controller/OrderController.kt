package com.kafkalab.order.controller

import com.kafkalab.order.model.CancelOrderRequest
import com.kafkalab.order.model.CreateOrderRequest
import com.kafkalab.order.model.OrderCancelledEvent
import com.kafkalab.order.model.OrderCreatedEvent
import com.kafkalab.order.service.OrderService
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/orders")
class OrderController(private val orderService: OrderService) {

    @PostMapping
    fun createOrder(@RequestBody request: CreateOrderRequest): ResponseEntity<OrderCreatedEvent> {
        val event = orderService.createOrder(
            userId = request.userId,
            product = request.product,
            quantity = request.quantity,
            totalAmount = request.totalAmount
        )
        return ResponseEntity.status(HttpStatus.CREATED).body(event)
    }

    @PostMapping("/{orderId}/cancel")
    fun cancelOrder(
        @PathVariable orderId: String,
        @RequestBody request: CancelOrderRequest
    ): ResponseEntity<OrderCancelledEvent> =
        ResponseEntity.ok(orderService.cancelOrder(orderId, request.userId, request.reason))
}