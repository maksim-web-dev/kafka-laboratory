package com.kafkalab.catalog.controller

import com.kafkalab.catalog.model.CreateProductRequest
import com.kafkalab.catalog.model.UpdateProductRequest
import com.kafkalab.catalog.service.ProductCatalogService
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException

@RestController
@RequestMapping("/api/products")
class ProductCatalogController(private val service: ProductCatalogService) {

    @GetMapping
    fun getAll() = service.getAll()

    @GetMapping("/{productId}")
    fun get(@PathVariable productId: String) =
        service.get(productId) ?: throw ResponseStatusException(HttpStatus.NOT_FOUND)

    @PostMapping
    fun create(@RequestBody req: CreateProductRequest) = service.create(req)

    @PatchMapping("/{productId}")
    fun update(@PathVariable productId: String, @RequestBody req: UpdateProductRequest) =
        service.update(productId, req) ?: throw ResponseStatusException(HttpStatus.NOT_FOUND)

    @DeleteMapping("/{productId}")
    fun delete(@PathVariable productId: String): Map<String, Any> {
        val deleted = service.delete(productId)
        if (!deleted) throw ResponseStatusException(HttpStatus.NOT_FOUND)
        return mapOf(
            "productId" to productId,
            "status" to "tombstone_sent",
            "note" to "Key will be removed from compacted log after delete.retention.ms (100ms)"
        )
    }

    @GetMapping("/compaction-info")
    fun compactionInfo() = mapOf(
        "topic" to "21.products",
        "cleanup.policy" to "compact",
        "min.cleanable.dirty.ratio" to "0.01",
        "delete.retention.ms" to "100",
        "segment.ms" to "10000",
        "behavior" to mapOf(
            "update" to "New record with same key — compaction retains only the latest",
            "delete" to "Send null value (tombstone) — compaction removes key after retention period",
            "read" to "Consumer reading from beginning sees latest state per key after compaction"
        )
    )
}