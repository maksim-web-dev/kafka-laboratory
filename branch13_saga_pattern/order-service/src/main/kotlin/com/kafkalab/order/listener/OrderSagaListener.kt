package com.kafkalab.order.listener

import com.kafkalab.avro.InventoryFailedEvent
import com.kafkalab.avro.InventoryReservedEvent
import com.kafkalab.avro.PaymentFailedEvent
import com.kafkalab.order.service.OrderSagaService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class OrderSagaListener(private val sagaService: OrderSagaService) {

    private val log = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["13.inventory.reserved"], groupId = "order-service-group")
    fun onInventoryReserved(record: ConsumerRecord<String, InventoryReservedEvent>) {
        val e = record.value()
        log.info("[ORDER-SAGA] InventoryReserved received — orderId={}", e.orderId)
        sagaService.confirm(e.orderId.toString())
    }

    @KafkaListener(topics = ["13.inventory.failed"], groupId = "order-service-group")
    fun onInventoryFailed(record: ConsumerRecord<String, InventoryFailedEvent>) {
        val e = record.value()
        log.info("[ORDER-SAGA] InventoryFailed received — orderId={} reason={}", e.orderId, e.reason)
        sagaService.cancel(e.orderId.toString(), "Inventory: ${e.reason}")
    }

    @KafkaListener(topics = ["13.payments.failed"], groupId = "order-service-group")
    fun onPaymentFailed(record: ConsumerRecord<String, PaymentFailedEvent>) {
        val e = record.value()
        log.info("[ORDER-SAGA] PaymentFailed received — orderId={} reason={}", e.orderId, e.reason)
        sagaService.cancel(e.orderId.toString(), "Payment: ${e.reason}")
    }
}