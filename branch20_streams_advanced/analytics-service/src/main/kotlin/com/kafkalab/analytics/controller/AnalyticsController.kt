package com.kafkalab.analytics.controller

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/analytics")
class AnalyticsController(private val streamsFactory: StreamsBuilderFactoryBean) {

    @GetMapping("/state")
    fun getState(): Map<String, String> {
        val streams = streamsFactory.kafkaStreams
        return mapOf(
            "state" to (streams?.state()?.name ?: "NOT_RUNNING"),
            "description" to "EOS processing.guarantee=exactly_once_v2"
        )
    }

    @GetMapping("/users/{userId}")
    fun getUser(@PathVariable userId: String): Map<String, Any?> {
        val streams = streamsFactory.kafkaStreams ?: return mapOf("error" to "streams not running")
        return try {
            val store = streams.store(
                StoreQueryParameters.fromNameAndType("users-store", QueryableStoreTypes.keyValueStore<String, Any>())
            )
            mapOf("userId" to userId, "profile" to store.get(userId))
        } catch (e: Exception) {
            mapOf("error" to e.message)
        }
    }

    @GetMapping("/products/{productId}")
    fun getProduct(@PathVariable productId: String): Map<String, Any?> {
        val streams = streamsFactory.kafkaStreams ?: return mapOf("error" to "streams not running")
        return try {
            val store = streams.store(
                StoreQueryParameters.fromNameAndType("products-global-store", QueryableStoreTypes.keyValueStore<String, Any>())
            )
            mapOf("productId" to productId, "product" to store.get(productId))
        } catch (e: Exception) {
            mapOf("error" to e.message)
        }
    }

    @GetMapping("/joins")
    fun getJoinInfo(): Map<String, Any> = mapOf(
        "join1" to mapOf(
            "type" to "KStream-KTable",
            "input" to "20.orders.created + 20.users",
            "output" to "20.enriched.orders",
            "note" to "Requires co-partitioning (same partition count). selectKey(userId) applied on stream."
        ),
        "join2" to mapOf(
            "type" to "KStream-GlobalKTable",
            "input" to "20.enriched.orders + 20.products",
            "output" to "20.enriched.products",
            "note" to "No co-partitioning needed. GlobalKTable replicates all data to every instance."
        ),
        "join3" to mapOf(
            "type" to "KStream-KStream windowed",
            "input" to "20.orders.created + 20.payments.processed",
            "output" to "20.matched.orders",
            "note" to "JoinWindows.ofTimeDifferenceWithNoGrace(5 min). Both keyed by orderId."
        )
    )
}