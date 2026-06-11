package com.kafkalab.order.service

import com.kafkalab.order.config.KafkaTopicConfig.Companion.TOPIC_ORDERS_CANCELLED
import com.kafkalab.order.config.KafkaTopicConfig.Companion.TOPIC_ORDERS_CREATED
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

    fun createOrder(userId: String, product: String, quantity: Int, totalAmount: Double): OrderCreatedEvent {
        val event = OrderCreatedEvent(userId = userId, product = product, quantity = quantity, totalAmount = totalAmount)

        kafkaTemplate.send(TOPIC_ORDERS_CREATED, event.orderId, event).whenComplete { result, ex ->
            if (ex != null) {
                log.error("Failed to publish OrderCreated {}: {}", event.orderId, ex.message)
            } else {
                val meta = result.recordMetadata
                log.info(
                    "OrderCreated published → topic={}, partition={}, offset={}, key={}",
                    meta.topic(), meta.partition(), meta.offset(), event.orderId
                )
            }
        }

        return event
    }

    fun cancelOrder(orderId: String, userId: String, reason: String): OrderCancelledEvent {
        val event = OrderCancelledEvent(orderId = orderId, userId = userId, reason = reason)

        // orders.cancelled has 1 partition — key не впливає на routing, але залишаємо для трасування
        kafkaTemplate.send(TOPIC_ORDERS_CANCELLED, event.orderId, event).whenComplete { result, ex ->
            if (ex != null) {
                log.error("Failed to publish OrderCancelled {}: {}", orderId, ex.message)
            } else {
                val meta = result.recordMetadata
                log.info(
                    "OrderCancelled published → topic={}, partition={}, offset={}, key={}",
                    meta.topic(), meta.partition(), meta.offset(), event.orderId
                )
            }
        }

        return event
    }
}