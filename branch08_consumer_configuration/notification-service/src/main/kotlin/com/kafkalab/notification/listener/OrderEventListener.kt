package com.kafkalab.notification.listener

import com.kafkalab.notification.model.OrderCancelledEvent
import com.kafkalab.notification.model.OrderCreatedEvent
import jakarta.annotation.PreDestroy
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.ConsumerSeekAware
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

@Component
class OrderEventListener(
    @Value("\${instance.id}") private val instanceId: String
) : ConsumerSeekAware {

    private val log = LoggerFactory.getLogger(javaClass)
    private val createdCount = AtomicInteger(0)
    private val cancelledCount = AtomicInteger(0)
    private val assignedPartitions: MutableSet<String> = ConcurrentHashMap.newKeySet()

    @KafkaListener(
        topics = ["08.orders.created"],
        groupId = "notification-service-group",
        properties = ["group.instance.id=\${instance.id}-orders-created"]
    )
    fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>, ack: Acknowledgment) {
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

        ack.acknowledge()
    }

    @KafkaListener(
        topics = ["08.orders.cancelled"],
        groupId = "notification-service-group",
        properties = ["group.instance.id=\${instance.id}-orders-cancelled"]
    )
    fun handleOrderCancelled(record: ConsumerRecord<String, OrderCancelledEvent>, ack: Acknowledgment) {
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

        ack.acknowledge()
    }

    override fun onPartitionsAssigned(
        assignments: Map<TopicPartition, Long>,
        callback: ConsumerSeekAware.ConsumerSeekCallback
    ) {
        val byTopic = assignments.keys.groupBy { it.topic() }
        byTopic.forEach { (topic, tps) ->
            val parts = tps.map { it.partition() }.sorted()
            log.info("[COOPERATIVE] Instance [{}] ASSIGNED: {}{}",
                instanceId, topic, parts.map { "[$it]" }.joinToString(""))
            parts.forEach { assignedPartitions.add("$topic[$it]") }
        }
    }

    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
        val byTopic = partitions.groupBy { it.topic() }
        byTopic.forEach { (topic, tps) ->
            val parts = tps.map { it.partition() }.sorted()
            log.info("[COOPERATIVE] Instance [{}] REVOKED: {}{}",
                instanceId, topic, parts.map { "[$it]" }.joinToString(""))
            parts.forEach { assignedPartitions.remove("$topic[$it]") }
        }
    }

    override fun onIdleContainer(
        assignments: Map<TopicPartition, Long>,
        callback: ConsumerSeekAware.ConsumerSeekCallback
    ) {}

    @PreDestroy
    fun onShutdown() {
        log.info("[GRACEFUL SHUTDOWN] Instance [{}] stopping — finishing current batch, committing offsets...", instanceId)
    }

    fun getCreatedCount(): Int = createdCount.get()
    fun getCancelledCount(): Int = cancelledCount.get()
    fun getInstanceId(): String = instanceId
    fun getAssignedPartitions(): List<String> = assignedPartitions.sorted()
}