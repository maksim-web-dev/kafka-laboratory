package com.kafkalab.notification.listener

import com.kafkalab.avro.OrderCreatedEvent
import com.kafkalab.avro.PaymentProcessedEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

// isolation-level: read-committed — цей consumer бачить лише committed транзакції.
// Повідомлення з відкочених транзакцій (abort) сюди ніколи не потраплять.
@Component
class NotificationListener {

    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["12.orders.created"], groupId = "notification-service-group")
    fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
        val e = record.value()
        val itemNames = e.items.joinToString { "${it.productName}(x${it.quantity})" }
        log.info("[NOTIFY] ORDER CREATED orderId={} userId={} items=[{}] total={}",
            e.orderId, e.userId, itemNames, e.totalAmount)
    }

    @KafkaListener(topics = ["12.payments.processed"], groupId = "notification-service-group")
    fun handlePaymentProcessed(record: ConsumerRecord<String, PaymentProcessedEvent>) {
        val e = record.value()
        log.info("[NOTIFY][read_committed] PAYMENT {} paymentId={} orderId={} amount={}",
            e.status, e.paymentId, e.orderId, e.amount)
    }
}