package com.kafkalab.payment.listener

import com.kafkalab.avro.OrderCreatedEvent
import com.kafkalab.avro.PaymentProcessedEvent
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

@Component
class OrderPaymentListener(private val kafkaTemplate: KafkaTemplate<String, SpecificRecord>) {

    private val log = LoggerFactory.getLogger(javaClass)
    private val failNextN = AtomicInteger(0)

    @KafkaListener(topics = ["11.orders.created"], groupId = "payment-service-group")
    fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
        val event = record.value()
        log.info("[PAYMENT] Processing orderId={} userId={} amount={}", event.orderId, event.userId, event.totalAmount)

        if (failNextN.get() > 0) {
            failNextN.decrementAndGet()
            throw RuntimeException("Simulated payment failure for order ${event.orderId}")
        }

        val payment = PaymentProcessedEvent.newBuilder()
            .setEventVersion("1.0")
            .setPaymentId(UUID.randomUUID().toString())
            .setOrderId(event.orderId.toString())
            .setUserId(event.userId.toString())
            .setAmount(event.totalAmount)
            .setStatus("APPROVED")
            .setTimestamp(LocalDateTime.now().toString())
            .build()

        kafkaTemplate.send("11.payments.processed", event.orderId.toString(), payment)
        log.info("[PAYMENT] Approved — orderId={} paymentId={}", event.orderId, payment.paymentId)
    }

    fun scheduleFailures(count: Int) {
        failNextN.set(count)
        log.warn("[PAYMENT] Next {} payment(s) will fail", count)
    }
}