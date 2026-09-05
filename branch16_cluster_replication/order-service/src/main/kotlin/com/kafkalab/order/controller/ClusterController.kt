package com.kafkalab.order.controller

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.springframework.beans.factory.annotation.Value
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/cluster")
class ClusterController(
    @Value("\${spring.kafka.bootstrap-servers}")
    private val bootstrapServers: String
) {

    private val topic = "16.orders.created"

    @GetMapping("/topic-info")
    fun topicInfo(): Any {
        AdminClient.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers)).use { admin ->
            val descriptions = admin.describeTopics(listOf(topic)).allTopicNames().get()
            val t = descriptions[topic] ?: return mapOf("error" to "Topic '$topic' not found")

            return mapOf(
                "topic" to topic,
                "partitions" to t.partitions().map { p ->
                    mapOf(
                        "partition" to p.partition(),
                        "leader" to (p.leader()?.id() ?: "none — leader unavailable!"),
                        "replicas" to p.replicas().map { it.id() },
                        "isr" to p.isr().map { it.id() },
                        "underReplicated" to (p.isr().size < p.replicas().size)
                    )
                }
            )
        }
    }

    @GetMapping("/brokers")
    fun brokers(): Any {
        AdminClient.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers)).use { admin ->
            val nodes = admin.describeCluster().nodes().get()
            val controller = admin.describeCluster().controller().get()
            return mapOf(
                "controller" to controller.id(),
                "brokers" to nodes.map { mapOf("id" to it.id(), "host" to it.host(), "port" to it.port()) }
            )
        }
    }
}