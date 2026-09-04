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
    private val abortNextN = AtomicInteger(0)

    @KafkaListener(topics = ["12.orders.created"], groupId = "payment-service-group")
    fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
        val event = record.value()
        val shouldAbort = abortNextN.get() > 0

        log.info("[PAYMENT-TX] Processing orderId={} transactional=true abort={}",
            event.orderId, shouldAbort)

        kafkaTemplate.executeInTransaction { kt ->
            val payment = PaymentProcessedEvent.newBuilder()
                .setEventVersion("1.0")
                .setPaymentId(UUID.randomUUID().toString())
                .setOrderId(event.orderId.toString())
                .setUserId(event.userId.toString())
                .setAmount(event.totalAmount)
                .setStatus("APPROVED")
                .setTimestamp(LocalDateTime.now().toString())
                .build()

            kt.send("12.payments.processed", event.orderId.toString(), payment)

            if (shouldAbort) {
                abortNextN.decrementAndGet()
                // Кидаємо виняток — транзакція відкочується, повідомлення не буде видно read_committed consumers
                throw RuntimeException("Simulated abort for orderId=${event.orderId} — transaction rolled back")
            }

            log.info("[PAYMENT-TX] Committed — orderId={} paymentId={} amount={}",
                event.orderId, payment.paymentId, payment.amount)
        }
    }

    fun scheduleAbort(count: Int) {
        abortNextN.set(count)
        log.warn("[PAYMENT-TX] Next {} transaction(s) will be aborted", count)
    }
}