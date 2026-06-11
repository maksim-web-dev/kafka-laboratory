package com.kafkalab.order.service

import com.kafkalab.order.model.OrderCreatedEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class OrderService(
    private val kafkaTemplate: KafkaTemplate<String, OrderCreatedEvent>
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val topic = "orders.created"

    fun createOrder(userId: String, product: String, quantity: Int, totalAmount: Double): OrderCreatedEvent {
        val event = OrderCreatedEvent(userId = userId, product = product, quantity = quantity, totalAmount = totalAmount)

        kafkaTemplate.send(topic, event.orderId, event).whenComplete { result, ex ->
            if (ex != null) {
                log.error("Failed to publish order {}: {}", event.orderId, ex.message)
            } else {
                val meta = result.recordMetadata
                log.info("Order published → topic={}, partition={}, offset={}", meta.topic(), meta.partition(), meta.offset())
            }
        }

        log.info("Order created: orderId={}, userId={}, product={}", event.orderId, userId, product)
        return event
    }
}