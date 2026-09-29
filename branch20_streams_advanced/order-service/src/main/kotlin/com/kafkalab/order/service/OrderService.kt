package com.kafkalab.order.service

import com.kafkalab.order.model.CreateOrderRequest
import com.kafkalab.order.model.OrderCreatedEvent
import com.kafkalab.order.model.PaymentProcessedEvent
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
        val order = OrderCreatedEvent(
            orderId = orderId,
            userId = req.userId,
            productId = req.productId,
            quantity = req.quantity,
            totalAmount = req.totalAmount,
            timestamp = LocalDateTime.now().toString()
        )
        kafkaTemplate.send("20.orders.created", orderId, order)
        log.info("[ORDER] published orderId={} userId={} productId={}", orderId, req.userId, req.productId)

        // Publish corresponding payment (simulated immediate approval)
        val payment = PaymentProcessedEvent(
            paymentId = UUID.randomUUID().toString(),
            orderId = orderId,
            userId = req.userId,
            amount = req.totalAmount,
            status = "APPROVED",
            timestamp = LocalDateTime.now().toString()
        )
        kafkaTemplate.send("20.payments.processed", orderId, payment)
        log.info("[PAYMENT] published paymentId={} orderId={} status=APPROVED", payment.paymentId, orderId)

        return order
    }

    fun createBatch(users: List<String>, count: Int): List<String> {
        val products = listOf("prod-laptop", "prod-phone", "prod-tablet", "prod-headphones")
        return (1..count).map {
            val req = CreateOrderRequest(
                userId = users[it % users.size],
                productId = products[it % products.size],
                quantity = 1,
                totalAmount = (50..2000).random().toDouble()
            )
            createOrder(req).orderId
        }
    }
}