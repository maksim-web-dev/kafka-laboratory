package com.kafkalab.order.service

import com.kafkalab.order.model.OrderCreatedEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.util.UUID
import kotlin.random.Random

@Service
class OrderService(private val kafkaTemplate: KafkaTemplate<String, Any>) {

    private val log = LoggerFactory.getLogger(javaClass)
    private val categories = listOf("ELECTRONICS", "FOOD", "CLOTHING", "GENERAL")
    private val topic = "16.orders.created"

    fun send(userId: String, totalAmount: Double, category: String): OrderCreatedEvent {
        val event = OrderCreatedEvent(
            orderId = UUID.randomUUID().toString(),
            userId = userId,
            totalAmount = totalAmount,
            category = category
        )
        // acks=all — waits for all ISR; throws if ISR < min.insync.replicas
        kafkaTemplate.send(topic, event.orderId, event).get()
        log.info("[REPL] Sent orderId={} userId={} acks=all OK", event.orderId, userId)
        return event
    }

    fun sendBatch(users: List<String>, count: Int): Map<String, Any> {
        var sent = 0
        var failed = 0
        repeat(count) {
            try {
                val user = users[Random.nextInt(users.size)]
                val amount = String.format("%.2f", Random.nextDouble(10.0, 300.0)).toDouble()
                send(user, amount, categories.random())
                sent++
            } catch (e: Exception) {
                failed++
                log.error("[REPL] Send failed: {}", e.cause?.message ?: e.message)
            }
        }
        return mapOf("requested" to count, "sent" to sent, "failed" to failed)
    }
}