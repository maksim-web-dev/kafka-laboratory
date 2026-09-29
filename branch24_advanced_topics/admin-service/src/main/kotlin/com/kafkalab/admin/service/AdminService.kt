package com.kafkalab.admin.service

import org.apache.kafka.clients.admin.*
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.concurrent.TimeUnit

@Service
class AdminService(private val adminClient: AdminClient) {

    private val log = LoggerFactory.getLogger(javaClass)

    fun listTopics(): List<Map<String, Any>> {
        val topics = adminClient.listTopics(ListTopicsOptions().listInternal(false))
            .listings().get(10, TimeUnit.SECONDS)
        return topics.map { mapOf("name" to it.name(), "internal" to it.isInternal) }
    }

    fun describeTopic(topicName: String): Map<String, Any> {
        val result = adminClient.describeTopics(listOf(topicName))
            .allTopicNames().get(10, TimeUnit.SECONDS)
        val desc = result[topicName] ?: return mapOf("error" to "topic not found")
        return mapOf(
            "name" to desc.name(),
            "partitions" to desc.partitions().map { p ->
                mapOf(
                    "partition" to p.partition(),
                    "leader" to p.leader()?.id(),
                    "replicas" to p.replicas().map { it.id() },
                    "isr" to p.isr().map { it.id() }
                )
            }
        )
    }

    fun createTopic(name: String, partitions: Int, replicationFactor: Short): Map<String, String> {
        val topic = NewTopic(name, partitions, replicationFactor)
        adminClient.createTopics(listOf(topic)).all().get(10, TimeUnit.SECONDS)
        log.info("[ADMIN] created topic={} partitions={}", name, partitions)
        return mapOf("status" to "created", "topic" to name, "partitions" to partitions.toString())
    }

    fun deleteTopic(name: String): Map<String, String> {
        adminClient.deleteTopics(listOf(name)).all().get(10, TimeUnit.SECONDS)
        log.info("[ADMIN] deleted topic={}", name)
        return mapOf("status" to "deleted", "topic" to name)
    }

    fun listConsumerGroups(): List<Map<String, Any>> {
        val groups = adminClient.listConsumerGroups().all().get(10, TimeUnit.SECONDS)
        return groups.map { mapOf("groupId" to it.groupId(), "state" to (it.state().map { s -> s.name }.orElse("UNKNOWN"))) }
    }

    fun describeConsumerGroup(groupId: String): Map<String, Any> {
        val result = adminClient.describeConsumerGroups(listOf(groupId))
            .all().get(10, TimeUnit.SECONDS)
        val desc = result[groupId] ?: return mapOf("error" to "group not found")
        return mapOf(
            "groupId" to desc.groupId(),
            "state" to desc.state().name,
            "members" to desc.members().map { m ->
                mapOf(
                    "memberId" to m.consumerId(),
                    "clientId" to m.clientId(),
                    "host" to m.host(),
                    "partitions" to m.assignment().topicPartitions().map { "${it.topic()}-${it.partition()}" }
                )
            }
        )
    }

    fun alterTopicConfig(topicName: String, retentionMs: Long): Map<String, String> {
        val resource = ConfigResource(ConfigResource.Type.TOPIC, topicName)
        // incrementalAlterConfigs: only changes what you specify, leaves the rest untouched
        val op = AlterConfigOp(
            ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, retentionMs.toString()),
            AlterConfigOp.OpType.SET
        )
        adminClient.incrementalAlterConfigs(mapOf(resource to listOf(op)))
            .all().get(10, TimeUnit.SECONDS)
        log.info("[ADMIN] altered topic={} retention.ms={}", topicName, retentionMs)
        return mapOf("status" to "updated", "topic" to topicName, "retention.ms" to retentionMs.toString())
    }

    fun getTopicConfig(topicName: String): Map<String, String> {
        val resource = ConfigResource(ConfigResource.Type.TOPIC, topicName)
        val result = adminClient.describeConfigs(listOf(resource))
            .all().get(10, TimeUnit.SECONDS)
        val config = result[resource] ?: return mapOf("error" to "not found")
        return config.entries().associate { it.name() to it.value() }
    }
}