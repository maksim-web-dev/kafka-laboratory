package com.kafkalab.order.interceptor

import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong

/**
 * ProducerInterceptor that counts sent messages and acks per topic.
 * Demonstrates onSend() for counting attempts and onAcknowledgement() for tracking outcomes.
 */
class MetricsProducerInterceptor : ProducerInterceptor<String, Any> {

    private val log = LoggerFactory.getLogger(javaClass)

    companion object {
        val sentCount = AtomicLong(0)
        val ackCount = AtomicLong(0)
        val errorCount = AtomicLong(0)
        val topicCounts = java.util.concurrent.ConcurrentHashMap<String, AtomicLong>()
    }

    override fun onSend(record: ProducerRecord<String, Any>): ProducerRecord<String, Any> {
        sentCount.incrementAndGet()
        topicCounts.computeIfAbsent(record.topic()) { AtomicLong(0) }.incrementAndGet()
        return record
    }

    override fun onAcknowledgement(metadata: RecordMetadata?, exception: Exception?) {
        if (exception == null) {
            ackCount.incrementAndGet()
            log.debug("[METRICS-INTERCEPTOR] ack partition={} offset={}", metadata?.partition(), metadata?.offset())
        } else {
            errorCount.incrementAndGet()
            log.warn("[METRICS-INTERCEPTOR] error: {}", exception.message)
        }
    }

    override fun close() {
        log.info("[METRICS-INTERCEPTOR] final stats: sent={} acked={} errors={}", sentCount.get(), ackCount.get(), errorCount.get())
    }

    override fun configure(configs: MutableMap<String, *>?) {}
}