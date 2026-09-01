package com.kafkalab.payment.listener

import com.kafkalab.payment.exception.PaymentException
import com.kafkalab.payment.model.OrderCreatedEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.DltHandler
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.RetryableTopic
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.retry.annotation.Backoff
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

@Component
class OrderPaymentListener {

    private val log = LoggerFactory.getLogger(javaClass)
    private val processedCount = AtomicInteger(0)
    private val dltCount = AtomicInteger(0)
    private val failNextN = AtomicInteger(0)

    @RetryableTopic(
        attempts = "3",
        backoff = Backoff(delay = 1000, multiplier = 5.0),
        topicSuffixingStrategy = org.springframework.kafka.retrytopic.TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        dltStrategy = org.springframework.kafka.retrytopic.DltStrategy.FAIL_ON_ERROR
    )
    @KafkaListener(topics = ["09.orders.created"], groupId = "payment-service-group")
    fun handleOrderCreated(
        record: ConsumerRecord<String, OrderCreatedEvent>,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String
    ) {
        val event = record.value()

        if (failNextN.get() > 0) {
            failNextN.decrementAndGet()
            log.warn("[RETRY] Attempt on topic={}, partition={}, offset={}, orderId={}",
                topic, record.partition(), record.offset(), event.orderId)
            throw PaymentException("Simulated payment failure for order ${event.orderId}")
        }

        val count = processedCount.incrementAndGet()
        log.info("╔══════════════════════════════════════════╗")
        log.info("║  PAYMENT PROCESSED  #{}", count)
        log.info("║  Topic     : {}", topic)
        log.info("║  Partition : {}  Offset: {}", record.partition(), record.offset())
        log.info("║  Order ID  : {}", event.orderId)
        log.info("║  User ID   : {}", event.userId)
        log.info("║  Amount    : \${}", event.totalAmount)
        log.info("║  → Payment charged successfully")
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
        log.error("║  ⚠  DEAD LETTER TOPIC  ⚠")
        log.error("║  Topic     : {}", topic)
        log.error("║  Partition : {}  Offset: {}", record.partition(), record.offset())
        log.error("║  Order ID  : {}", event.orderId)
        log.error("║  User ID   : {}", event.userId)
        log.error("║  Amount    : \${}", event.totalAmount)
        log.error("║  → Manual intervention required!")
        log.error("╚══════════════════════════════════════════╝")
    }

    fun setFailNextN(n: Int) = failNextN.set(n)
    fun getProcessedCount(): Int = processedCount.get()
    fun getDltCount(): Int = dltCount.get()
    fun getFailNextN(): Int = failNextN.get()
}