package com.kafkalab.order.interceptor

import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.LoggerFactory
import java.nio.charset.StandardCharsets

/**
 * ProducerInterceptor that injects standard headers into every outgoing record.
 *
 * onSend()          — called before serialization; mutate or replace the record here
 * onAcknowledgement() — called after broker ack (or on error); used for metrics/logging
 */
class HeaderEnrichmentInterceptor : ProducerInterceptor<String, Any> {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun onSend(record: ProducerRecord<String, Any>): ProducerRecord<String, Any> {
        // Inject tracing/service headers into every message
        record.headers().add("x-source-service", "order-service".toByteArray(StandardCharsets.UTF_8))
        record.headers().add("x-schema-version", "1".toByteArray(StandardCharsets.UTF_8))
        record.headers().add("x-sent-at", System.currentTimeMillis().toString().toByteArray(StandardCharsets.UTF_8))
        log.debug("[HEADER-INTERCEPTOR] enriched record key={} topic={}", record.key(), record.topic())
        return record
    }

    override fun onAcknowledgement(metadata: RecordMetadata?, exception: Exception?) {
        if (exception != null) {
            log.error("[HEADER-INTERCEPTOR] ack error topic={} err={}", metadata?.topic(), exception.message)
        }
    }

    override fun close() {}

    override fun configure(configs: MutableMap<String, *>?) {}
}