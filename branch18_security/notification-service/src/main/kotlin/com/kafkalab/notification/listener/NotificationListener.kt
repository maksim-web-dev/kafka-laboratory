package com.kafkalab.notification.listener

import com.kafkalab.notification.model.OrderCreatedEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class NotificationListener {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["18.orders.created"], groupId = "notification-service-group")
    fun onOrder(event: OrderCreatedEvent) {
        log.info("[SEC] Received orderId={} userId={} amount={} — user=notification-consumer",
            event.orderId, event.userId, event.totalAmount)
    }
}