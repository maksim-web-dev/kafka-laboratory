package com.kafkalab.order.service

import com.kafkalab.order.model.BatchOrderRequest
import com.kafkalab.order.model.OrderCancelledEvent
import com.kafkalab.order.model.OrderCreatedEvent
import com.kafkalab.order.model.OrderSendResult
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class OrderService(
    private val kafkaTemplate: KafkaTemplate<String, Any>
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val topicCreated = "08.orders.created"
    private val topicCancelled = "08.orders.cancelled"

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

    fun cancelOrder(orderId: String, userId: String, reason: String): OrderCancelledEvent {
        val event = OrderCancelledEvent(orderId = orderId, userId = userId, reason = reason)

        kafkaTemplate.send(topicCancelled, userId, event).whenComplete { result, ex ->
            if (ex != null) {
                log.error("Failed to publish cancellation {}: {}", orderId, ex.message)
            } else {
                val meta = result.recordMetadata
                log.info("OrderCancelled published → topic={}, key={}, partition={}, offset={}",
                    meta.topic(), userId, meta.partition(), meta.offset())
            }
        }

        log.info("Order cancelled: orderId={}, userId={}, reason={}", orderId, userId, reason)
        return event
    }

    fun createBatch(request: BatchOrderRequest): List<OrderSendResult> {
        return (1..request.count).map { i ->
            val event = OrderCreatedEvent(
                userId = request.userId,
                product = "${request.product} #$i",
                quantity = 1,
                totalAmount = (i * 10).toDouble()
            )
            val result = kafkaTemplate.send(topicCreated, request.userId, event).get()
            val meta = result.recordMetadata
            log.info("OrderCreated published → topic={}, key={}, partition={}, offset={}",
                meta.topic(), request.userId, meta.partition(), meta.offset())
            OrderSendResult(orderId = event.orderId, userId = request.userId, key = request.userId,
                partition = meta.partition(), offset = meta.offset())
        }
    }
}