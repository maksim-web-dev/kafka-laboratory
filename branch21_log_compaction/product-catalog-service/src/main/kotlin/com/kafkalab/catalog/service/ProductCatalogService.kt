package com.kafkalab.catalog.service

import com.kafkalab.catalog.model.CreateProductRequest
import com.kafkalab.catalog.model.ProductEvent
import com.kafkalab.catalog.model.UpdateProductRequest
import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentHashMap

@Service
class ProductCatalogService(private val kafkaTemplate: KafkaTemplate<String, Any>) {

    private val log = LoggerFactory.getLogger(javaClass)
    private val localCache = ConcurrentHashMap<String, ProductEvent>()

    @PostConstruct
    fun seedCatalog() {
        listOf(
            ProductEvent("prod-laptop", "Laptop Pro 15", "Electronics", 1299.99, 50, "High-performance laptop"),
            ProductEvent("prod-phone", "Smartphone X", "Electronics", 699.99, 200, "Latest flagship phone"),
            ProductEvent("prod-tablet", "Tablet Air", "Electronics", 499.99, 75, "Lightweight tablet"),
            ProductEvent("prod-headphones", "Headphones Max", "Audio", 199.99, 150, "Premium over-ear headphones"),
            ProductEvent("prod-keyboard", "Mechanical Keyboard", "Peripherals", 89.99, 300, "Cherry MX switches")
        ).forEach { create(it) }
    }

    fun create(req: CreateProductRequest): ProductEvent {
        val productId = "prod-${req.name.lowercase().replace(" ", "-").take(20)}"
        val event = ProductEvent(productId, req.name, req.category, req.price, req.stock, req.description)
        return create(event)
    }

    private fun create(event: ProductEvent): ProductEvent {
        localCache[event.productId] = event
        // Each send with same key overwrites previous value due to log compaction
        kafkaTemplate.send("21.products", event.productId, event)
        log.info("[CATALOG] created/updated productId={} name={}", event.productId, event.name)
        return event
    }

    fun update(productId: String, req: UpdateProductRequest): ProductEvent? {
        val existing = localCache[productId] ?: return null
        val updated = existing.copy(
            name = req.name ?: existing.name,
            category = req.category ?: existing.category,
            price = req.price ?: existing.price,
            stock = req.stock ?: existing.stock,
            description = req.description ?: existing.description
        )
        localCache[productId] = updated
        // New record with same key — compaction will keep only this latest value
        kafkaTemplate.send("21.products", productId, updated)
        log.info("[CATALOG] updated productId={} price={}", productId, updated.price)
        return updated
    }

    fun delete(productId: String): Boolean {
        if (!localCache.containsKey(productId)) return false
        localCache.remove(productId)
        // Tombstone record: null value with existing key
        // Compaction removes the key entirely after delete.retention.ms passes
        kafkaTemplate.send("21.products", productId, null)
        log.info("[CATALOG] tombstone sent for productId={}", productId)
        return true
    }

    fun getAll(): Map<String, ProductEvent> = localCache.toMap()

    fun get(productId: String): ProductEvent? = localCache[productId]
}