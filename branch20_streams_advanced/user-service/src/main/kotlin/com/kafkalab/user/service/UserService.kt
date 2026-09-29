package com.kafkalab.user.service

import com.kafkalab.user.model.ProductEvent
import com.kafkalab.user.model.UpsertUserRequest
import com.kafkalab.user.model.UserProfile
import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class UserService(private val kafkaTemplate: KafkaTemplate<String, Any>) {

    private val log = LoggerFactory.getLogger(javaClass)

    @PostConstruct
    fun seedUsers() {
        listOf(
            UserProfile("user-1", "Alice Smith", "alice@example.com", "PREMIUM", "UA"),
            UserProfile("user-2", "Bob Jones", "bob@example.com", "STANDARD", "PL"),
            UserProfile("user-3", "Carol White", "carol@example.com", "VIP", "DE"),
            UserProfile("user-4", "Dan Brown", "dan@example.com", "STANDARD", "UA"),
            UserProfile("user-5", "Eve Davis", "eve@example.com", "PREMIUM", "FR")
        ).forEach { upsert(it) }

        listOf(
            ProductEvent("prod-laptop", "Laptop Pro 15", "Electronics", 1299.99),
            ProductEvent("prod-phone", "Smartphone X", "Electronics", 699.99),
            ProductEvent("prod-tablet", "Tablet Air", "Electronics", 499.99),
            ProductEvent("prod-headphones", "Headphones Max", "Audio", 199.99)
        ).forEach { publishProduct(it) }
    }

    fun upsert(req: UpsertUserRequest, userId: String): UserProfile {
        val profile = UserProfile(userId, req.name, req.email, req.tier, req.country)
        return upsert(profile)
    }

    private fun upsert(profile: UserProfile): UserProfile {
        kafkaTemplate.send("20.users", profile.userId, profile)
        log.info("[USER] upserted userId={} name={}", profile.userId, profile.name)
        return profile
    }

    fun delete(userId: String) {
        // tombstone: null value signals compaction to remove the key
        kafkaTemplate.send("20.users", userId, null)
        log.info("[USER] tombstone sent for userId={}", userId)
    }

    private fun publishProduct(event: ProductEvent) {
        kafkaTemplate.send("20.products", event.productId, event)
        log.info("[PRODUCT] published productId={} category={}", event.productId, event.category)
    }

    fun publishProduct(req: ProductEvent): ProductEvent {
        publishProduct(req)
        return req
    }
}