package com.kafkalab.reader.controller

import com.kafkalab.reader.listener.CatalogListener
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/catalog")
class CatalogReaderController(private val listener: CatalogListener) {

    @GetMapping
    fun getCatalog() = listener.catalog

    @GetMapping("/{productId}")
    fun getProduct(@PathVariable productId: String) =
        listener.catalog[productId] ?: mapOf("error" to "not found")

    @GetMapping("/tombstones")
    fun getTombstones() = mapOf(
        "tombstones" to listener.tombstones,
        "note" to "Keys that received null value (deletion marker for log compaction)"
    )

    @GetMapping("/stats")
    fun getStats() = mapOf(
        "totalProducts" to listener.catalog.size,
        "tombstonesReceived" to listener.tombstones.size,
        "compactionNote" to "Consumer reads from offset=0 and sees only latest value per key after compaction runs"
    )
}