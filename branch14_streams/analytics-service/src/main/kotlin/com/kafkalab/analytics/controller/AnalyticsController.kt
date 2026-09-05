package com.kafkalab.analytics.controller

import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.springframework.http.ResponseEntity
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/analytics")
class AnalyticsController(private val factoryBean: StreamsBuilderFactoryBean) {

    private fun ordersStore(): ReadOnlyKeyValueStore<String, Long> =
        factoryBean.kafkaStreams.store(
            StoreQueryParameters.fromNameAndType(
                "orders-by-user-store",
                QueryableStoreTypes.keyValueStore<String, Long>()
            )
        )

    // Всі userId з кількістю замовлень
    @GetMapping("/orders-by-user")
    fun ordersByUser(): ResponseEntity<Any> {
        return try {
            val store = ordersStore()
            val result = mutableMapOf<String, Long>()
            store.all().use { iter -> iter.forEach { kv -> result[kv.key] = kv.value } }
            ResponseEntity.ok(result)
        } catch (e: InvalidStateStoreException) {
            ResponseEntity.status(503).body(mapOf("error" to "Store not ready yet, retry in a few seconds"))
        }
    }

    // Топ-N userId за кількістю замовлень
    @GetMapping("/top-users")
    fun topUsers(@RequestParam(defaultValue = "3") limit: Int): ResponseEntity<Any> {
        return try {
            val store = ordersStore()
            val result = mutableListOf<Map<String, Any>>()
            store.all().use { iter ->
                iter.asSequence()
                    .sortedByDescending { it.value }
                    .take(limit)
                    .forEach { kv -> result.add(mapOf("userId" to kv.key, "orderCount" to kv.value)) }
            }
            ResponseEntity.ok(result)
        } catch (e: InvalidStateStoreException) {
            ResponseEntity.status(503).body(mapOf("error" to "Store not ready yet, retry in a few seconds"))
        }
    }
}