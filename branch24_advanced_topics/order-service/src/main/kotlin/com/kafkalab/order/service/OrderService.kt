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
            quantity = req.quantity,
            totalAmount = req.totalAmount,
            timestamp = LocalDateTime.now().toString()
        )
        // HeaderEnrichmentInterceptor and MetricsProducerInterceptor are applied automatically
        kafkaTemplate.send("24.orders.created", orderId, event)
        log.info("[ORDER-ADVANCED] published orderId={} userId={}", orderId, req.userId)
        return event
    }
}