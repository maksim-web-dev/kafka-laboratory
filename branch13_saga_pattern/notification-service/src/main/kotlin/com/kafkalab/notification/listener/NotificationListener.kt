package com.kafkalab.notification.listener

import com.kafkalab.avro.InventoryFailedEvent
import com.kafkalab.avro.InventoryReservedEvent
import com.kafkalab.avro.OrderCancelledEvent
import com.kafkalab.avro.OrderConfirmedEvent
import com.kafkalab.avro.OrderCreatedEvent
import com.kafkalab.avro.PaymentFailedEvent
import com.kafkalab.avro.PaymentProcessedEvent
import com.kafkalab.avro.PaymentRefundedEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class NotificationListener {

    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["13.orders.created"], groupId = "notification-service-group")
    fun onOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
        val e = record.value()
        log.info("[SAGA-STEP 1/5] ORDER CREATED   orderId={} userId={} total={}", e.orderId, e.userId, e.totalAmount)
    }

    @KafkaListener(topics = ["13.payments.processed"], groupId = "notification-service-group")
    fun onPaymentProcessed(record: ConsumerRecord<String, PaymentProcessedEvent>) {
        val e = record.value()
        log.info("[SAGA-STEP 2/5] PAYMENT APPROVED orderId={} paymentId={} amount={}", e.orderId, e.paymentId, e.amount)
    }

    @KafkaListener(topics = ["13.payments.failed"], groupId = "notification-service-group")
    fun onPaymentFailed(record: ConsumerRecord<String, PaymentFailedEvent>) {
        val e = record.value()
        log.warn("[SAGA-COMP ❌]  PAYMENT FAILED   orderId={} reason={}", e.orderId, e.reason)
    }

    @KafkaListener(topics = ["13.inventory.reserved"], groupId = "notification-service-group")
    fun onInventoryReserved(record: ConsumerRecord<String, InventoryReservedEvent>) {
        val e = record.value()
        log.info("[SAGA-STEP 3/5] INVENTORY RESERVED orderId={}", e.orderId)
    }

    @KafkaListener(topics = ["13.inventory.failed"], groupId = "notification-service-group")
    fun onInventoryFailed(record: ConsumerRecord<String, InventoryFailedEvent>) {
        val e = record.value()
        log.warn("[SAGA-COMP ❌]  INVENTORY FAILED  orderId={} reason={}", e.orderId, e.reason)
    }

    @KafkaListener(topics = ["13.payments.refunded"], groupId = "notification-service-group")
    fun onPaymentRefunded(record: ConsumerRecord<String, PaymentRefundedEvent>) {
        val e = record.value()
        log.warn("[SAGA-COMP 💰]  PAYMENT REFUNDED  orderId={} amount={}", e.orderId, e.amount)
    }

    @KafkaListener(topics = ["13.orders.confirmed"], groupId = "notification-service-group")
    fun onOrderConfirmed(record: ConsumerRecord<String, OrderConfirmedEvent>) {
        val e = record.value()
        log.info("[SAGA-DONE  ✅] ORDER CONFIRMED  orderId={} userId={}", e.orderId, e.userId)
    }

    @KafkaListener(topics = ["13.orders.cancelled"], groupId = "notification-service-group")
    fun onOrderCancelled(record: ConsumerRecord<String, OrderCancelledEvent>) {
        val e = record.value()
        log.warn("[SAGA-DONE  ❌] ORDER CANCELLED  orderId={} reason={}", e.orderId, e.reason)
    }
}