package com.kafkalab.notification.controller

import com.kafkalab.notification.listener.OrderEventListener
import org.springframework.beans.factory.annotation.Value
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/notifications")
class NotificationController(
    private val listener: OrderEventListener,
    @Value("\${spring.kafka.consumer.group-id}") private val groupId: String,
    @Value("\${spring.kafka.consumer.max-poll-records}") private val maxPollRecords: Int,
    @Value("\${spring.kafka.consumer.properties.max\\.poll\\.interval\\.ms}") private val maxPollIntervalMs: Int,
    @Value("\${spring.kafka.consumer.properties.session\\.timeout\\.ms}") private val sessionTimeoutMs: Int,
    @Value("\${spring.kafka.consumer.properties.heartbeat\\.interval\\.ms}") private val heartbeatIntervalMs: Int,
    @Value("\${instance.id}") private val instanceId: String
) {

    @GetMapping("/count")
    fun count(): Map<String, Any> {
        val created = listener.getCreatedCount()
        val cancelled = listener.getCancelledCount()
        return mapOf(
            "instanceId" to listener.getInstanceId(),
            "orders_created" to created,
            "orders_cancelled" to cancelled,
            "total" to created + cancelled
        )
    }

    @GetMapping("/partitions")
    fun partitions(): Map<String, Any> {
        val parts = listener.getAssignedPartitions()
        return mapOf(
            "instanceId" to instanceId,
            "assignedPartitions" to parts,
            "count" to parts.size
        )
    }

    @GetMapping("/config")
    fun config(): Map<String, Any> = mapOf(
        "instanceId" to instanceId,
        "groupId" to groupId,
        "staticMemberId_created" to "$instanceId-orders-created",
        "staticMemberId_cancelled" to "$instanceId-orders-cancelled",
        "assignmentStrategy" to "CooperativeStickyAssignor",
        "maxPollRecords" to maxPollRecords,
        "maxPollIntervalMs" to maxPollIntervalMs,
        "sessionTimeoutMs" to sessionTimeoutMs,
        "heartbeatIntervalMs" to heartbeatIntervalMs,
        "ackMode" to "MANUAL_IMMEDIATE",
        "enableAutoCommit" to false,
        "gracefulShutdown" to true
    )
}