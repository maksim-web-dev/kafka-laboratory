package com.kafkalab.order.service

import com.kafkalab.order.model.OrderCancelledEvent
import com.kafkalab.order.model.OrderCreatedEvent
import com.kafkalab.order.model.OrderItem
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class OrderService(
    private val kafkaTemplate: KafkaTemplate<String, Any>
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val topicCreated = "10.orders.created"
    private val topicCancelled = "10.orders.cancelled"

    fun createOrder(userId: String, product: String, quantity: Int, totalAmount: Double): OrderCreatedEvent {
        val item = OrderItem(productName = product, quantity = quantity, unitPrice = totalAmount / quantity)
        val event = OrderCreatedEvent(userId = userId, items = listOf(item), totalAmount = totalAmount)

        kafkaTemplate.send(topicCreated, userId, event).whenComplete { result, ex ->
            if (ex != null) {
                log.error("Failed to publish order {}: {}", event.orderId, ex.message)
            } else {
                val meta = result.recordMetadata
                log.info("OrderCreated published → topic={}, partition={}, offset={}, version={}",
                    meta.topic(), meta.partition(), meta.offset(), event.eventVersion)
            }
        }
        return event
    }

    fun cancelOrder(orderId: String, userId: String, reason: String): OrderCancelledEvent {
        val event = OrderCancelledEvent(orderId = orderId, userId = userId, reason = reason)

        kafkaTemplate.send(topicCancelled, userId, event).whenComplete { result, ex ->
            if (ex != null) {
                log.error("Failed to publish cancellation {}: {}", orderId, ex.message)
            } else {
                val meta = result.recordMetadata
                log.info("OrderCancelled published → topic={}, partition={}, offset={}",
                    meta.topic(), meta.partition(), meta.offset())
            }
        }
        return event
    }
}