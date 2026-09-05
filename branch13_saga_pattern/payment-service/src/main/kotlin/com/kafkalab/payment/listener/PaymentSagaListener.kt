package com.kafkalab.payment.listener

import com.kafkalab.avro.InventoryFailedEvent
import com.kafkalab.avro.OrderCreatedEvent
import com.kafkalab.avro.PaymentFailedEvent
import com.kafkalab.avro.PaymentProcessedEvent
import com.kafkalab.avro.PaymentRefundedEvent
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.time.LocalDateTime
import java.util.Collections
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

@Component
class PaymentSagaListener(private val kafkaTemplate: KafkaTemplate<String, SpecificRecord>) {

    private val log = LoggerFactory.getLogger(javaClass)
    private val failNextN = AtomicInteger(0)

    // idempotency: запам'ятовуємо оброблені ключі щоб уникнути дублювання при retry
    private val processedKeys = Collections.newSetFromMap(ConcurrentHashMap<String, Boolean>())
    // orderId → (userId, amount) для compensation
    private val payments = ConcurrentHashMap<String, Pair<String, Double>>()

    @KafkaListener(topics = ["13.orders.created"], groupId = "payment-service-group")
    fun onOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
        val e = record.value()

        if (!processedKeys.add(e.idempotencyKey.toString())) {
            log.warn("[PAYMENT-SAGA] Duplicate idempotencyKey={} — skipped", e.idempotencyKey)
            return
        }

        if (failNextN.get() > 0) {
            failNextN.decrementAndGet()
            val failed = PaymentFailedEvent.newBuilder()
                .setEventVersion("1.0")
                .setOrderId(e.orderId.toString())
                .setUserId(e.userId.toString())
                .setReason("Simulated payment failure")
                .setTimestamp(LocalDateTime.now().toString())
                .build()
            kafkaTemplate.send("13.payments.failed", e.orderId.toString(), failed)
            log.warn("[PAYMENT-SAGA] FAILED orderId={}", e.orderId)
            return
        }

        payments[e.orderId.toString()] = Pair(e.userId.toString(), e.totalAmount)

        val processed = PaymentProcessedEvent.newBuilder()
            .setEventVersion("1.0")
            .setPaymentId(UUID.randomUUID().toString())
            .setOrderId(e.orderId.toString())
            .setUserId(e.userId.toString())
            .setAmount(e.totalAmount)
            .setTimestamp(LocalDateTime.now().toString())
            .build()

        kafkaTemplate.send("13.payments.processed", e.orderId.toString(), processed)
        log.info("[PAYMENT-SAGA] APPROVED orderId={} amount={}", e.orderId, e.totalAmount)
    }

    // Compensation: inventory failed → refund payment
    @KafkaListener(topics = ["13.inventory.failed"], groupId = "payment-service-group")
    fun onInventoryFailed(record: ConsumerRecord<String, InventoryFailedEvent>) {
        val e = record.value()
        val (userId, amount) = payments[e.orderId.toString()] ?: return

        val refunded = PaymentRefundedEvent.newBuilder()
            .setEventVersion("1.0")
            .setOrderId(e.orderId.toString())
            .setUserId(userId)
            .setAmount(amount)
            .setTimestamp(LocalDateTime.now().toString())
            .build()

        kafkaTemplate.send("13.payments.refunded", e.orderId.toString(), refunded)
        log.info("[PAYMENT-COMPENSATE] Refunded orderId={} amount={}", e.orderId, amount)
    }

    fun scheduleFailure(count: Int) {
        failNextN.set(count)
        log.warn("[PAYMENT-SAGA] Next {} payment(s) will fail", count)
    }
}