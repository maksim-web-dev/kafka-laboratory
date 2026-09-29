package com.kafkalab.order.controller

import com.kafkalab.order.model.CreateOrderRequest
import com.kafkalab.order.service.OrderService
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/orders")
class OrderController(private val orderService: OrderService) {

    @PostMapping
    fun createOrder(@RequestBody req: CreateOrderRequest) = orderService.createOrder(req)

    @GetMapping("/security-info")
    fun securityInfo() = mapOf(
        "protocol" to "SASL_SSL",
        "mechanism" to "PLAIN",
        "user" to "order-producer",
        "acl" to "WRITE on topic 23.orders.created",
        "tls" to mapOf(
            "truststore" to "/certs/client.truststore.jks",
            "endpoint_identification" to "disabled (self-signed cert)",
            "note" to "In production use ssl.endpoint.identification.algorithm=https"
        )
    )
}