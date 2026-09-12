package com.kafkalab.inventory.listener

import com.kafkalab.avro.InventoryFailedEvent
import com.kafkalab.avro.InventoryReservedEvent
import com.kafkalab.avro.PaymentProcessedEvent
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger

@Component
class InventorySagaListener(private val kafkaTemplate: KafkaTemplate<String, SpecificRecord>) {

    private val log = LoggerFactory.getLogger(javaClass)
    private val outOfStockNextN = AtomicInteger(0)

    // Manual commit: offset acknowledged only after successful publish to next saga step
    @KafkaListener(topics = ["19.payments.processed"], groupId = "inventory-service-group")
    fun onPaymentProcessed(record: ConsumerRecord<String, PaymentProcessedEvent>, ack: Acknowledgment) {
        val e = record.value()

        if (outOfStockNextN.get() > 0) {
            outOfStockNextN.decrementAndGet()
            val failed = InventoryFailedEvent.newBuilder()
                .setEventVersion("1.0")
                .setOrderId(e.orderId.toString())
                .setUserId(e.userId.toString())
                .setReason("Out of stock (simulated)")
                .setTimestamp(LocalDateTime.now().toString())
                .build()
            kafkaTemplate.send("19.inventory.failed", e.orderId.toString(), failed)
            log.warn("[INVENTORY-SAGA] OUT OF STOCK orderId={} → compensation triggered", e.orderId)
            ack.acknowledge()
            return
        }

        val reserved = InventoryReservedEvent.newBuilder()
            .setEventVersion("1.0")
            .setOrderId(e.orderId.toString())
            .setUserId(e.userId.toString())
            .setTimestamp(LocalDateTime.now().toString())
            .build()
        kafkaTemplate.send("19.inventory.reserved", e.orderId.toString(), reserved)
        log.info("[INVENTORY-SAGA] RESERVED orderId={}", e.orderId)
        // Commit offset only after successful event publish
        ack.acknowledge()
    }

    fun scheduleOutOfStock(count: Int) {
        outOfStockNextN.set(count)
        log.warn("[INVENTORY-SAGA] Next {} reservation(s) will fail (out of stock)", count)
    }
}