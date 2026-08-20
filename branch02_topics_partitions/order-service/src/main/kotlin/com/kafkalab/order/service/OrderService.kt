package com.kafkalab.order.service

import com.kafkalab.order.model.OrderCancelledEvent
import com.kafkalab.order.model.OrderCreatedEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class OrderService(
    private val kafkaTemplate: KafkaTemplate<String, Any>
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val topicCreated = "02.orders.created"
    private val topicCancelled = "02.orders.cancelled"

    fun createOrder(userId: String, product: String, quantity: Int, totalAmount: Double): OrderCreatedEvent {
        val event = OrderCreatedEvent(userId = userId, product = product, quantity = quantity, totalAmount = totalAmount)

        kafkaTemplate.send(topicCreated, event.orderId, event).whenComplete { result, ex ->
            if (ex != null) {
                log.error("Failed to publish order {}: {}", event.orderId, ex.message)
            } else {
                val meta = result.recordMetadata
                log.info("OrderCreated published → topic={}, partition={}, offset={}, key={}", meta.topic(), meta.partition(), meta.offset(), event.orderId)
            }
        }

        log.info("Order created: orderId={}, userId={}, product={}", event.orderId, userId, product)
        return event
    }

    fun cancelOrder(orderId: String, userId: String, reason: String): OrderCancelledEvent {
        val event = OrderCancelledEvent(orderId = orderId, userId = userId, reason = reason)

        kafkaTemplate.send(topicCancelled, orderId, event).whenComplete { result, ex ->
            if (ex != null) {
                log.error("Failed to publish cancellation {}: {}", orderId, ex.message)
            } else {
                val meta = result.recordMetadata
                log.info("OrderCancelled published → topic={}, partition={}, offset={}, key={}", meta.topic(), meta.partition(), meta.offset(), orderId)
            }
        }

        log.info("Order cancelled: orderId={}, userId={}, reason={}", orderId, userId, reason)
        return event
    }
}