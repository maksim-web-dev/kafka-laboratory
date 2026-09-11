package com.kafkalab.order.service

import com.kafkalab.order.model.OrderCreatedEvent
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.util.UUID

@Service
class OrderService(
    private val kafkaTemplate: KafkaTemplate<String, OrderCreatedEvent>,
    @Value("\${app.topic}") private val topic: String
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun send(userId: String, totalAmount: Double, category: String): OrderCreatedEvent {
        val event = OrderCreatedEvent(
            orderId = UUID.randomUUID().toString(),
            userId = userId,
            totalAmount = totalAmount,
            category = category
        )
        kafkaTemplate.send(topic, event.orderId, event).get()
        log.info("[SEC] Sent orderId={} user=order-producer → topic={}", event.orderId, topic)
        return event
    }
}