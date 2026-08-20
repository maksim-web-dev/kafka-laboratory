package com.kafkalab.notification.listener

import com.kafkalab.notification.model.OrderCancelledEvent
import com.kafkalab.notification.model.OrderCreatedEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

@Component
class OrderEventListener {

    private val log = LoggerFactory.getLogger(javaClass)
    private val createdCount = AtomicInteger(0)
    private val cancelledCount = AtomicInteger(0)

    @KafkaListener(topics = ["02.orders.created"], groupId = "notification-service-group")
    fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
        val event = record.value()
        val count = createdCount.incrementAndGet()

        log.info("──────────────────────────────────────")
        log.info(" ORDER CREATED #{}", count)
        log.info(" Partition: {}  |  Offset: {}", record.partition(), record.offset())
        log.info(" Order ID  : {}", event.orderId)
        log.info(" User ID   : {}", event.userId)
        log.info(" Product   : {} x{}", event.product, event.quantity)
        log.info(" Total     : \${}", event.totalAmount)
        log.info(" Sent at   : {}", event.timestamp)
        log.info(" → Email sent to user {}", event.userId)
        log.info("──────────────────────────────────────")
    }

    @KafkaListener(topics = ["02.orders.cancelled"], groupId = "notification-service-group")
    fun handleOrderCancelled(record: ConsumerRecord<String, OrderCancelledEvent>) {
        val event = record.value()
        val count = cancelledCount.incrementAndGet()

        log.info("──────────────────────────────────────")
        log.info(" ORDER CANCELLED #{}", count)
        log.info(" Partition: {}  |  Offset: {}", record.partition(), record.offset())
        log.info(" Order ID  : {}", event.orderId)
        log.info(" User ID   : {}", event.userId)
        log.info(" Reason    : {}", event.reason)
        log.info(" Sent at   : {}", event.timestamp)
        log.info(" → Cancellation email sent to user {}", event.userId)
        log.info("──────────────────────────────────────")
    }

    fun getCreatedCount(): Int = createdCount.get()
    fun getCancelledCount(): Int = cancelledCount.get()
}