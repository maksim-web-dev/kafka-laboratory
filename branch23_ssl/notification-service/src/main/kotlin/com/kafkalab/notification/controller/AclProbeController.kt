package com.kafkalab.notification.controller

import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/acl")
class AclProbeController {

    @GetMapping("/info")
    fun getAclInfo() = mapOf(
        "consumer" to mapOf(
            "user" to "notification-consumer",
            "acl" to listOf(
                "ALLOW READ on topic 23.orders.created",
                "ALLOW READ on group notification-group"
            )
        ),
        "security" to mapOf(
            "protocol" to "SASL_SSL",
            "mechanism" to "PLAIN (username/password over TLS)",
            "tls_version" to "TLSv1.3",
            "certificate_type" to "self-signed CA (dev only)"
        ),
        "concepts" to mapOf(
            "ACL" to "Access Control List — per-user ALLOW/DENY rules on Kafka resources",
            "SASL" to "Authentication: verifies WHO is connecting",
            "SSL/TLS" to "Encryption: secures data in transit",
            "SASL_SSL" to "Both: authenticated + encrypted connection"
        )
    )
}