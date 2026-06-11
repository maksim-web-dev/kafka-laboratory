package com.kafkalab.notification.listener

import com.kafkalab.notification.model.OrderCreatedEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

@Component
class OrderEventListener {

    private val log = LoggerFactory.getLogger(javaClass)
    private val receivedCount = AtomicInteger(0)

    @KafkaListener(topics = ["orders.created"], groupId = "notification-service-group")
    fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
        val event = record.value()
        val count = receivedCount.incrementAndGet()

        log.info("──────────────────────────────────────")
        log.info(" NOTIFICATION #{}", count)
        log.info(" Partition: {}  |  Offset: {}", record.partition(), record.offset())
        log.info(" Order ID  : {}", event.orderId)
        log.info(" User ID   : {}", event.userId)
        log.info(" Product   : {} x{}", event.product, event.quantity)
        log.info(" Total     : \${}", event.totalAmount)
        log.info(" Sent at   : {}", event.timestamp)
        log.info(" → Email sent to user {}", event.userId)
        log.info("──────────────────────────────────────")
    }

    fun getReceivedCount(): Int = receivedCount.get()
}