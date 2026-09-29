package com.kafkalab.reader.listener

import com.kafkalab.reader.model.ProductEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

@Component
class CatalogListener {

    private val log = LoggerFactory.getLogger(javaClass)
    val catalog = ConcurrentHashMap<String, ProductEvent>()
    val tombstones = mutableListOf<String>()

    @KafkaListener(topics = ["21.products"], groupId = "catalog-reader-group")
    fun onProductEvent(record: ConsumerRecord<String, ProductEvent?>) {
        val key = record.key()
        val value = record.value()

        if (value == null) {
            // Tombstone record: null value means the key was deleted
            catalog.remove(key)
            tombstones.add(key)
            log.info("[READER] tombstone received for productId={} — removed from local state", key)
        } else {
            // Upsert: add or replace. After compaction only latest value per key survives.
            catalog[key] = value
            log.info("[READER] received productId={} name={} partition={} offset={}",
                key, value.name, record.partition(), record.offset())
        }
    }
}