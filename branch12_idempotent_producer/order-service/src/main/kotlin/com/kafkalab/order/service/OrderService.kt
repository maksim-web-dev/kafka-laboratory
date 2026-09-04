package com.kafkalab.order.service

import com.kafkalab.avro.OrderCreatedEvent
import com.kafkalab.avro.OrderItem
import org.apache.avro.specific.SpecificRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.util.UUID

@Service
class OrderService(private val kafkaTemplate: KafkaTemplate<String, SpecificRecord>) {

    private val log = LoggerFactory.getLogger(javaClass)

    fun createOrder(userId: String, itemCount: Int = 2): String {
        val orderId = UUID.randomUUID().toString()
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
            .setTimestamp(LocalDateTime.now().toString())
            .build()

        kafkaTemplate.send("12.orders.created", orderId, event)
        log.info("[IDEMPOTENT-PRODUCER] OrderCreated — orderId={} userId={} items={} total={}",
            orderId, userId, items.size, event.totalAmount)
        return orderId
    }
}