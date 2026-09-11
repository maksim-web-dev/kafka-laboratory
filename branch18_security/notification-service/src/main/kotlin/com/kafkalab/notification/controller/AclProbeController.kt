package com.kafkalab.notification.controller

import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.concurrent.TimeUnit

@RestController
@RequestMapping("/api/probe")
class AclProbeController(private val kafkaTemplate: KafkaTemplate<String, String>) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Demonstrates ACL enforcement: notification-consumer has READ-only ACL,
     * so any attempt to produce will be rejected with TopicAuthorizationException.
     */
    @PostMapping("/write-attempt")
    fun attemptUnauthorizedWrite(): Map<String, Any> {
        log.warn("[ACL] Attempting unauthorized WRITE as notification-consumer...")
        return try {
            kafkaTemplate.send("18.orders.created", "probe", "unauthorized-write").get(5, TimeUnit.SECONDS)
            log.error("[ACL] Unexpected: write SUCCEEDED — check ACL configuration!")
            mapOf("result" to "ALLOWED", "message" to "Unexpected: write succeeded — ACLs may not be configured correctly")
        } catch (e: Exception) {
            val root: Throwable = generateSequence(e as Throwable) { it.cause }.last()
            log.warn("[ACL] Write DENIED as expected: {}", root.message)
            mapOf(
                "result" to "DENIED",
                "exception" to root.javaClass.simpleName,
                "message" to (root.message ?: "Authorization failed"),
                "explanation" to "notification-consumer has READ ACL only — WRITE is forbidden by Kafka ACL"
            )
        }
    }
}