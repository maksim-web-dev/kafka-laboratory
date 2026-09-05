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
        log.info("[OBS] Sent orderId={} userId={}", event.orderId, userId)
        return event
    }

    fun sendFlood(count: Int): Map<String, Any> {
        val users = listOf("user-1", "user-2", "user-3")
        val categories = listOf("ELECTRONICS", "CLOTHING", "FOOD")
        repeat(count) { i ->
            val event = OrderCreatedEvent(
                orderId = UUID.randomUUID().toString(),
                userId = users[i % users.size],
                totalAmount = (10..500).random().toDouble(),
                category = categories[i % categories.size]
            )
            // Fire-and-forget: no .get() — builds lag fast
            kafkaTemplate.send(topic, event.orderId, event)
        }
        kafkaTemplate.flush()
        log.info("[OBS] Flood: sent {} messages to {}", count, topic)
        return mapOf("sent" to count, "topic" to topic)
    }
}