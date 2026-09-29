package com.kafkalab.notification.listener

import com.kafkalab.notification.model.NotificationRecord
import com.kafkalab.notification.model.OrderCreatedEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.time.LocalDateTime
import java.util.concurrent.CopyOnWriteArrayList

@Component
class OrderEventListener {

    private val log = LoggerFactory.getLogger(javaClass)
    val notifications = CopyOnWriteArrayList<NotificationRecord>()

    @KafkaListener(topics = ["23.orders.created"], groupId = "notification-group")
    fun onOrderCreated(event: OrderCreatedEvent) {
        val notification = NotificationRecord(
            orderId = event.orderId,
            userId = event.userId,
            message = "Order ${event.orderId} confirmed for user ${event.userId} — amount: ${event.totalAmount}",
            receivedAt = LocalDateTime.now().toString()
        )
        notifications.add(notification)
        // Consumer connected via SASL_SSL — message received over encrypted channel
        log.info("[NOTIFICATION-SSL] received orderId={} userId={} via SASL_SSL consumer", event.orderId, event.userId)
    }
}