package com.kafkalab.admin.controller

import com.kafkalab.admin.service.AdminService
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/admin")
class AdminController(private val adminService: AdminService) {

    @GetMapping("/topics")
    fun listTopics() = adminService.listTopics()

    @GetMapping("/topics/{topicName}")
    fun describeTopic(@PathVariable topicName: String) = adminService.describeTopic(topicName)

    @PostMapping("/topics")
    fun createTopic(
        @RequestParam name: String,
        @RequestParam(defaultValue = "3") partitions: Int,
        @RequestParam(defaultValue = "1") replicationFactor: Short
    ) = adminService.createTopic(name, partitions, replicationFactor)

    @DeleteMapping("/topics/{topicName}")
    fun deleteTopic(@PathVariable topicName: String) = adminService.deleteTopic(topicName)

    @GetMapping("/consumer-groups")
    fun listConsumerGroups() = adminService.listConsumerGroups()

    @GetMapping("/consumer-groups/{groupId}")
    fun describeConsumerGroup(@PathVariable groupId: String) =
        adminService.describeConsumerGroup(groupId)

    @PatchMapping("/topics/{topicName}/retention")
    fun alterRetention(
        @PathVariable topicName: String,
        @RequestParam retentionMs: Long
    ) = adminService.alterTopicConfig(topicName, retentionMs)

    @GetMapping("/topics/{topicName}/config")
    fun getTopicConfig(@PathVariable topicName: String) =
        adminService.getTopicConfig(topicName)

    @GetMapping("/concepts")
    fun getConcepts() = mapOf(
        "AdminClient" to mapOf(
            "createTopics()" to "Programmatically create topics with partitions/replication",
            "deleteTopics()" to "Delete topics",
            "describeTopics()" to "Get partition/replica/ISR details",
            "listConsumerGroups()" to "List all consumer groups in the cluster",
            "describeConsumerGroups()" to "Get members, assignments, lag per group",
            "incrementalAlterConfigs()" to "Change topic config without affecting other settings",
            "describeConfigs()" to "Read current topic/broker configuration"
        ),
        "Quotas" to mapOf(
            "producer_byte_rate" to "Max bytes/sec per producer client",
            "consumer_byte_rate" to "Max bytes/sec per consumer client",
            "request_percentage" to "Max % of request handler threads one client can use",
            "set_via" to "kafka-configs.sh --alter --add-config 'producer_byte_rate=1048576' --entity-type users"
        )
    )
}