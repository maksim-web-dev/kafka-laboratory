package com.kafkalab.notification.listener

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component

@Component
class NotificationListener {

    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["16.orders.created"], groupId = "notification-service-group")
    fun onOrder(
        record: ConsumerRecord<String, Map<String, Any>>,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        @Header(KafkaHeaders.OFFSET) offset: Long
    ) {
        val v = record.value()
        log.info(
            "[REPL] Received orderId={} userId={} amount={} | partition={} offset={}",
            v["orderId"], v["userId"], v["totalAmount"], partition, offset
        )
    }
}