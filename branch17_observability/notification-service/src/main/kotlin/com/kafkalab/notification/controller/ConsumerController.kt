package com.kafkalab.notification.controller

import com.kafkalab.notification.service.ConsumerStateService
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/consumer")
class ConsumerController(private val state: ConsumerStateService) {

    @PostMapping("/pause")
    fun pause(): Map<String, Any> {
        state.paused = true
        return mapOf("paused" to true, "message" to "Consumer paused — lag will grow in Grafana")
    }

    @PostMapping("/resume")
    fun resume(): Map<String, Any> {
        state.paused = false
        state.processingDelayMs = 0L
        return mapOf("paused" to false, "message" to "Consumer resumed — lag will drain")
    }

    @PostMapping("/slow")
    fun slow(@RequestParam(defaultValue = "2000") ms: Long): Map<String, Any> {
        state.paused = false
        state.processingDelayMs = ms
        return mapOf("delayMs" to ms, "message" to "Consumer slowed to ${ms}ms per message")
    }

    @PostMapping("/fast")
    fun fast(): Map<String, Any> {
        state.paused = false
        state.processingDelayMs = 0L
        return mapOf("delayMs" to 0, "message" to "Consumer running at full speed")
    }
}