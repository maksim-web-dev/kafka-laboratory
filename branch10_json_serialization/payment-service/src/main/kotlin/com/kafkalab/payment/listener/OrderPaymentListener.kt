package com.kafkalab.payment.listener

import com.kafkalab.payment.exception.PaymentException
import com.kafkalab.payment.model.OrderCreatedEvent
import com.kafkalab.payment.model.PaymentProcessedEvent
import com.kafkalab.payment.model.PaymentStatus
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.DltHandler
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.RetryableTopic
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.retrytopic.DltStrategy
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.retry.annotation.Backoff
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

@Component
class OrderPaymentListener(
    private val kafkaTemplate: KafkaTemplate<String, Any>
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val processedCount = AtomicInteger(0)
    private val dltCount = AtomicInteger(0)
    private val failNextN = AtomicInteger(0)
    private val topicProcessed = "10.payments.processed"

    @RetryableTopic(
        attempts = "3",
        backoff = Backoff(delay = 1000, multiplier = 5.0),
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        dltStrategy = DltStrategy.FAIL_ON_ERROR
    )
    @KafkaListener(topics = ["10.orders.created"], groupId = "payment-service-group")
    fun handleOrderCreated(
        record: ConsumerRecord<String, OrderCreatedEvent>,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String
    ) {
        val event = record.value()

        if (failNextN.get() > 0) {
            failNextN.decrementAndGet()
            log.warn("[RETRY] Attempt on topic={}, orderId={}, eventVersion={}",
                topic, event.orderId, event.eventVersion)
            throw PaymentException("Simulated payment failure for order ${event.orderId}")
        }

        val count = processedCount.incrementAndGet()
        val payment = PaymentProcessedEvent(
            orderId = event.orderId,
            userId = event.userId,
            amount = event.totalAmount,
            status = PaymentStatus.APPROVED
        )

        kafkaTemplate.send(topicProcessed, event.userId, payment).whenComplete { result, ex ->
            if (ex != null) {
                log.error("Failed to publish PaymentProcessed: {}", ex.message)
            } else {
                val meta = result.recordMetadata
                log.info("PaymentProcessed published → topic={}, partition={}, offset={}, status={}",
                    meta.topic(), meta.partition(), meta.offset(), payment.status)
            }
        }

        log.info("╔══════════════════════════════════════════╗")
        log.info("║  PAYMENT PROCESSED  #{} [{}]", count, payment.status)
        log.info("║  Topic (received) : {}", topic)
        log.info("║  eventVersion     : {}", event.eventVersion)
        log.info("║  Order ID  : {}", event.orderId)
        log.info("║  Items     : {}", event.items.joinToString { "${it.productName} x${it.quantity}" })
        log.info("║  Amount    : \${}", event.totalAmount)
        log.info("╚══════════════════════════════════════════╝")
    }

    @DltHandler
    fun handleDlt(
        record: ConsumerRecord<String, OrderCreatedEvent>,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String
    ) {
        val event = record.value()
        dltCount.incrementAndGet()

        log.error("╔══════════════════════════════════════════╗")
        log.error("║  ⚠  DEAD LETTER — publishing DECLINED  ⚠")
        log.error("║  Topic     : {}", topic)
        log.error("║  Order ID  : {}", event.orderId)
        log.error("╚══════════════════════════════════════════╝")

        val payment = PaymentProcessedEvent(
            orderId = event.orderId,
            userId = event.userId,
            amount = event.totalAmount,
            status = PaymentStatus.DECLINED
        )
        kafkaTemplate.send(topicProcessed, event.userId, payment)
    }

    fun setFailNextN(n: Int) = failNextN.set(n)
    fun getProcessedCount(): Int = processedCount.get()
    fun getDltCount(): Int = dltCount.get()
    fun getFailNextN(): Int = failNextN.get()
}