package com.kafkalab.order.service

import com.kafkalab.order.model.OrderCreatedEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class OrderService(
    private val kafkaTemplate: KafkaTemplate<String, Any>
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val topicCreated = "09.orders.created"

    fun createOrder(userId: String, product: String, quantity: Int, totalAmount: Double): OrderCreatedEvent {
        val event = OrderCreatedEvent(userId = userId, product = product, quantity = quantity, totalAmount = totalAmount)

        kafkaTemplate.send(topicCreated, userId, event).whenComplete { result, ex ->
            if (ex != null) {
                log.error("Failed to publish order {}: {}", event.orderId, ex.message)
            } else {
                val meta = result.recordMetadata
                log.info("OrderCreated published → topic={}, key={}, partition={}, offset={}",
                    meta.topic(), userId, meta.partition(), meta.offset())
            }
        }

        log.info("Order created: orderId={}, userId={}, product={}", event.orderId, userId, product)
        return event
    }
}