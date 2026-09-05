package com.kafkalab.notification.listener

import com.kafkalab.notification.model.OrderCreatedEvent
import com.kafkalab.notification.service.ConsumerStateService
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class NotificationListener(private val state: ConsumerStateService) {
    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["17.orders.created"], groupId = "notification-service-group")
    fun onOrder(event: OrderCreatedEvent) {
        while (state.paused) {
            Thread.sleep(200)
        }
        if (state.processingDelayMs > 0) {
            Thread.sleep(state.processingDelayMs)
        }
        log.info("[LAG] Processed orderId={} userId={} amount={}", event.orderId, event.userId, event.totalAmount)
    }
}