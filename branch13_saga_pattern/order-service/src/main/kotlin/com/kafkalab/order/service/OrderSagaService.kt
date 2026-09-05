package com.kafkalab.order.service

import com.kafkalab.avro.OrderCancelledEvent
import com.kafkalab.avro.OrderConfirmedEvent
import com.kafkalab.avro.OrderCreatedEvent
import com.kafkalab.avro.OrderItem
import org.apache.avro.specific.SpecificRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@Service
class OrderSagaService(private val kafkaTemplate: KafkaTemplate<String, SpecificRecord>) {

    private val log = LoggerFactory.getLogger(javaClass)

    // orderId → status
    private val orders = ConcurrentHashMap<String, String>()
    // orderId → userId (для compensation-подій)
    private val orderUsers = ConcurrentHashMap<String, String>()

    fun createOrder(userId: String, itemCount: Int = 2): String {
        val orderId = UUID.randomUUID().toString()
        val idempotencyKey = "order-created:$orderId"

        val items = (1..itemCount).map {
            OrderItem.newBuilder()
                .setProductId(UUID.randomUUID().toString())
                .setProductName("Product-$it")
                .setQuantity(it)
                .setUnitPrice(it * 9.99)
                .build()
        }
        val event = OrderCreatedEvent.newBuilder()
            .setEventVersion("1.0")
            .setOrderId(orderId)
            .setUserId(userId)
            .setItems(items)
            .setTotalAmount(items.sumOf { it.quantity * it.unitPrice })
            .setIdempotencyKey(idempotencyKey)
            .setTimestamp(LocalDateTime.now().toString())
            .build()

        orders[orderId] = "PENDING"
        orderUsers[orderId] = userId
        kafkaTemplate.send("13.orders.created", orderId, event)
        log.info("[SAGA-START] orderId={} userId={} total={}", orderId, userId, event.totalAmount)
        return orderId
    }

    fun confirm(orderId: String) {
        val userId = orderUsers[orderId] ?: return
        orders[orderId] = "CONFIRMED"

        val event = OrderConfirmedEvent.newBuilder()
            .setEventVersion("1.0")
            .setOrderId(orderId)
            .setUserId(userId)
            .setTimestamp(LocalDateTime.now().toString())
            .build()

        kafkaTemplate.send("13.orders.confirmed", orderId, event)
        log.info("[SAGA-COMPLETE] orderId={} status=CONFIRMED", orderId)
    }

    fun cancel(orderId: String, reason: String) {
        val userId = orderUsers[orderId] ?: return
        orders[orderId] = "CANCELLED"

        val event = OrderCancelledEvent.newBuilder()
            .setEventVersion("1.0")
            .setOrderId(orderId)
            .setUserId(userId)
            .setReason(reason)
            .setTimestamp(LocalDateTime.now().toString())
            .build()

        kafkaTemplate.send("13.orders.cancelled", orderId, event)
        log.info("[SAGA-COMPENSATE] orderId={} status=CANCELLED reason={}", orderId, reason)
    }

    fun getStatus(orderId: String): String = orders[orderId] ?: "NOT_FOUND"
}