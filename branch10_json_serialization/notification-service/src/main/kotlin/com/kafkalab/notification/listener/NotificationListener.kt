package com.kafkalab.notification.listener

import com.kafkalab.notification.model.OrderCancelledEvent
import com.kafkalab.notification.model.OrderCreatedEvent
import com.kafkalab.notification.model.PaymentProcessedEvent
import com.kafkalab.notification.model.PaymentStatus
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

@Component
class NotificationListener(
    @Value("\${instance.id}") private val instanceId: String
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val createdCount = AtomicInteger(0)
    private val cancelledCount = AtomicInteger(0)
    private val paymentsCount = AtomicInteger(0)

    @KafkaListener(topics = ["10.orders.created"], groupId = "notification-service-group")
    fun handleOrderCreated(
        record: ConsumerRecord<String, OrderCreatedEvent>,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String
    ) {
        val event = record.value()
        val count = createdCount.incrementAndGet()

        log.info("╔══════════════════════════════════════════╗")
        log.info("║  ORDER CREATED  #{} [v{}]", count, event.eventVersion)
        log.info("║  Topic     : {} (TypeId header: OrderCreatedEvent)", topic)
        log.info("║  Order ID  : {}", event.orderId)
        log.info("║  User ID   : {}", event.userId)
        log.info("║  Items     : {}", event.items.joinToString { "${it.productName} x${it.quantity}" })
        log.info("║  Total     : \${}", event.totalAmount)
        log.info("║  → Order confirmation email sent")
        log.info("╚══════════════════════════════════════════╝")
    }

    @KafkaListener(topics = ["10.orders.cancelled"], groupId = "notification-service-group")
    fun handleOrderCancelled(
        record: ConsumerRecord<String, OrderCancelledEvent>,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String
    ) {
        val event = record.value()
        val count = cancelledCount.incrementAndGet()

        log.info("╔══════════════════════════════════════════╗")
        log.info("║  ORDER CANCELLED  #{} [v{}]", count, event.eventVersion)
        log.info("║  Order ID  : {}", event.orderId)
        log.info("║  Reason    : {}", event.reason)
        log.info("║  → Cancellation email sent")
        log.info("╚══════════════════════════════════════════╝")
    }

    @KafkaListener(topics = ["10.payments.processed"], groupId = "notification-service-group")
    fun handlePaymentProcessed(
        record: ConsumerRecord<String, PaymentProcessedEvent>,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String
    ) {
        val event = record.value()
        val count = paymentsCount.incrementAndGet()
        val emoji = if (event.status == PaymentStatus.APPROVED) "✓ APPROVED" else "✗ DECLINED"

        log.info("╔══════════════════════════════════════════╗")
        log.info("║  PAYMENT  #{} — {}", count, emoji)
        log.info("║  Topic     : {} (TypeId header: PaymentProcessedEvent)", topic)
        log.info("║  eventVersion : {}", event.eventVersion)
        log.info("║  Payment ID: {}", event.paymentId)
        log.info("║  Order ID  : {}", event.orderId)
        log.info("║  Amount    : \${}", event.amount)
        log.info("║  → Payment {} email sent to {}", event.status, event.userId)
        log.info("╚══════════════════════════════════════════╝")
    }

    fun getCreatedCount(): Int = createdCount.get()
    fun getCancelledCount(): Int = cancelledCount.get()
    fun getPaymentsCount(): Int = paymentsCount.get()
    fun getInstanceId(): String = instanceId
}