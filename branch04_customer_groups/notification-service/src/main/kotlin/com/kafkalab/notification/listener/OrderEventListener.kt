package com.kafkalab.notification.listener

import com.kafkalab.notification.model.OrderCancelledEvent
import com.kafkalab.notification.model.OrderCreatedEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.ConsumerSeekAware
import org.springframework.stereotype.Component
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

@Component
class OrderEventListener(
    @Value("\${instance.id}") private val instanceId: String
) : ConsumerSeekAware {

    private val log = LoggerFactory.getLogger(javaClass)
    private val createdCount = AtomicInteger(0)
    private val cancelledCount = AtomicInteger(0)
    private val assignedPartitions = CopyOnWriteArrayList<TopicPartition>()

    @KafkaListener(topics = ["04.orders.created"], groupId = "notification-service-group")
    fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
        val event = record.value()
        val count = createdCount.incrementAndGet()

        log.info("╔══════════════════════════════════════════╗")
        log.info("║  ORDER CREATED  #{}", count)
        log.info("║  Instance  : {}", instanceId)
        log.info("║  Key: {}  →  Partition: {}  Offset: {}", record.key(), record.partition(), record.offset())
        log.info("║  Order ID  : {}", event.orderId)
        log.info("║  User ID   : {}", event.userId)
        log.info("║  Product   : {} x{}", event.product, event.quantity)
        log.info("║  Total     : \${}", event.totalAmount)
        log.info("║  → Email sent to user {}", event.userId)
        log.info("╚══════════════════════════════════════════╝")
    }

    @KafkaListener(topics = ["04.orders.cancelled"], groupId = "notification-service-group")
    fun handleOrderCancelled(record: ConsumerRecord<String, OrderCancelledEvent>) {
        val event = record.value()
        val count = cancelledCount.incrementAndGet()

        log.info("╔══════════════════════════════════════════╗")
        log.info("║  ORDER CANCELLED  #{}", count)
        log.info("║  Instance  : {}", instanceId)
        log.info("║  Key: {}  →  Partition: {}  Offset: {}", record.key(), record.partition(), record.offset())
        log.info("║  Order ID  : {}", event.orderId)
        log.info("║  User ID   : {}", event.userId)
        log.info("║  Reason    : {}", event.reason)
        log.info("║  → Cancellation email sent to user {}", event.userId)
        log.info("╚══════════════════════════════════════════╝")
    }

    override fun onPartitionsAssigned(
        assignments: Map<TopicPartition, Long>,
        callback: ConsumerSeekAware.ConsumerSeekCallback
    ) {
        assignedPartitions.addAll(assignments.keys)
        val sorted = assignments.keys.sortedBy { "${it.topic()}[${it.partition()}]" }
        log.info("[ASSIGNED] Instance [{}] ASSIGNED: {}", instanceId, sorted)
    }

    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
        assignedPartitions.removeAll(partitions.toSet())
        val sorted = partitions.sortedBy { "${it.topic()}[${it.partition()}]" }
        log.info("[REBALANCE] Instance [{}] REVOKED:  {}", instanceId, sorted)
    }

    override fun registerSeekCallback(callback: ConsumerSeekAware.ConsumerSeekCallback) {}

    fun getCreatedCount(): Int = createdCount.get()
    fun getCancelledCount(): Int = cancelledCount.get()
    fun getAssignedPartitions(): List<TopicPartition> = assignedPartitions.toList()
    fun getInstanceId(): String = instanceId
}