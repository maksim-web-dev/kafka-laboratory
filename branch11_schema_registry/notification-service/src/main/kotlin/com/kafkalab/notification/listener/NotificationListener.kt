package com.kafkalab.notification.listener

import com.kafkalab.avro.OrderCancelledEvent
import com.kafkalab.avro.OrderCreatedEvent
import com.kafkalab.avro.PaymentProcessedEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class NotificationListener {

    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["11.orders.created"], groupId = "notification-service-group")
    fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
        val e = record.value()
        val itemNames = e.items.joinToString { "${it.productName}(x${it.quantity})" }
        log.info("[NOTIFY] ORDER CREATED v={} orderId={} userId={} items=[{}] total={}",
            e.eventVersion, e.orderId, e.userId, itemNames, e.totalAmount)
    }

    @KafkaListener(topics = ["11.orders.cancelled"], groupId = "notification-service-group")
    fun handleOrderCancelled(record: ConsumerRecord<String, OrderCancelledEvent>) {
        val e = record.value()
        log.info("[NOTIFY] ORDER CANCELLED v={} orderId={} userId={} reason={}",
            e.eventVersion, e.orderId, e.userId, e.reason)
    }

    @KafkaListener(topics = ["11.payments.processed"], groupId = "notification-service-group")
    fun handlePaymentProcessed(record: ConsumerRecord<String, PaymentProcessedEvent>) {
        val e = record.value()
        log.info("[NOTIFY] PAYMENT {} v={} paymentId={} orderId={} amount={}",
            e.status, e.eventVersion, e.paymentId, e.orderId, e.amount)
    }
}