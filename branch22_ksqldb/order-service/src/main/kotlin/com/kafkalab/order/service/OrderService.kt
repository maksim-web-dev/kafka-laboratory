package com.kafkalab.order.service

import com.kafkalab.order.model.CreateOrderRequest
import com.kafkalab.order.model.OrderCreatedEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.util.UUID

@Service
class OrderService(private val kafkaTemplate: KafkaTemplate<String, Any>) {

    private val log = LoggerFactory.getLogger(javaClass)

    fun createOrder(req: CreateOrderRequest): OrderCreatedEvent {
        val orderId = UUID.randomUUID().toString()
        val event = OrderCreatedEvent(
            orderId = orderId,
            userId = req.userId,
            productId = req.productId,
            category = req.category,
            quantity = req.quantity,
            totalAmount = req.totalAmount,
            timestamp = LocalDateTime.now().toString()
        )
        kafkaTemplate.send("22.orders.created", orderId, event)
        log.info("[ORDER] published orderId={} userId={} category={}", orderId, req.userId, req.category)
        return event
    }

    fun createBatch(users: List<String>, count: Int): List<String> {
        val products = listOf(
            Triple("prod-laptop", "Electronics", 1299.99),
            Triple("prod-phone", "Electronics", 699.99),
            Triple("prod-tablet", "Electronics", 499.99),
            Triple("prod-headphones", "Audio", 199.99)
        )
        return (1..count).map {
            val (productId, category, price) = products[it % products.size]
            createOrder(CreateOrderRequest(
                userId = users[it % users.size],
                productId = productId,
                category = category,
                quantity = 1,
                totalAmount = price
            )).orderId
        }
    }
}