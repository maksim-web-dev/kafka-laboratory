package com.kafkalab.analytics.streams

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.kafkalab.analytics.model.*
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.Stores
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class KafkaStreamsTopology(private val objectMapper: ObjectMapper) {

    private val log = LoggerFactory.getLogger(javaClass)

    @Autowired
    fun buildTopology(builder: StreamsBuilder) {
        buildKStreamKTableJoin(builder)
        buildKStreamGlobalKTableJoin(builder)
        buildKStreamKStreamWindowedJoin(builder)
    }

    /**
     * JOIN 1: KStream-KTable join (co-partitioned)
     *
     * Orders stream joined with Users KTable.
     * Requirement: both topics must have the same number of partitions
     * and orders must be keyed by userId (selectKey).
     *
     * Result: EnrichedOrder with user details written to 20.enriched.orders
     */
    private fun buildKStreamKTableJoin(builder: StreamsBuilder) {
        // KTable from 20.users topic (key=userId)
        val usersTable: KTable<String, UserProfile> = builder.table(
            "20.users",
            Consumed.with(Serdes.String(), jsonSerde<UserProfile>()),
            Materialized.`as`<String, UserProfile>(
                Stores.persistentKeyValueStore("users-store")
            ).withKeySerde(Serdes.String()).withValueSerde(jsonSerde<UserProfile>())
        )

        // Orders stream re-keyed by userId for co-partitioning
        val ordersStream: KStream<String, OrderCreatedEvent> = builder
            .stream("20.orders.created", Consumed.with(Serdes.String(), jsonSerde<OrderCreatedEvent>()))
            .selectKey { _, order ->
                log.debug("[JOIN1] rekeying orderId={} to userId={}", order.orderId, order.userId)
                order.userId
            }

        // KStream-KTable join: lookup user for each order
        ordersStream
            .join(
                usersTable,
                { order, user ->
                    if (user == null) {
                        log.warn("[JOIN1] no user found for userId={}, orderId={}", order.userId, order.orderId)
                        null
                    } else {
                        EnrichedOrder(
                            orderId = order.orderId,
                            userId = order.userId,
                            userName = user.name,
                            userTier = user.tier,
                            productId = order.productId,
                            productName = order.productId,  // will be enriched further in JOIN2
                            productCategory = "UNKNOWN",
                            totalAmount = order.totalAmount,
                            timestamp = order.timestamp
                        ).also { log.info("[JOIN1] KStream-KTable enriched orderId={} with user={}", order.orderId, user.name) }
                    }
                },
                Joined.with(Serdes.String(), jsonSerde<OrderCreatedEvent>(), jsonSerde<UserProfile>())
            )
            .filter { _, enriched -> enriched != null }
            .to("20.enriched.orders", Produced.with(Serdes.String(), jsonSerde<EnrichedOrder>()))
    }

    /**
     * JOIN 2: KStream-GlobalKTable join (no co-partitioning needed)
     *
     * Enriched orders stream joined with Products GlobalKTable.
     * GlobalKTable replicates ALL partitions to every Streams instance —
     * no need to selectKey, and topics don't need same partition count.
     *
     * Result: EnrichedOrder with product details written to 20.enriched.products
     */
    private fun buildKStreamGlobalKTableJoin(builder: StreamsBuilder) {
        // GlobalKTable from 20.products (key=productId) — full replication to every instance
        val productsGlobalTable: GlobalKTable<String, ProductEvent> = builder.globalTable(
            "20.products",
            Consumed.with(Serdes.String(), jsonSerde<ProductEvent>()),
            Materialized.`as`<String, ProductEvent>(
                Stores.persistentKeyValueStore("products-global-store")
            ).withKeySerde(Serdes.String()).withValueSerde(jsonSerde<ProductEvent>())
        )

        // Read already-enriched orders from 20.enriched.orders
        val enrichedStream: KStream<String, EnrichedOrder> = builder
            .stream("20.enriched.orders", Consumed.with(Serdes.String(), jsonSerde<EnrichedOrder>()))

        // KStream-GlobalKTable join: KeyValueMapper extracts the join key from the stream record
        enrichedStream
            .join(
                productsGlobalTable,
                // KeyValueMapper: (streamKey, streamValue) -> globalTableKey
                { _, enrichedOrder -> enrichedOrder.productId },
                { enrichedOrder, product ->
                    if (product == null) {
                        log.warn("[JOIN2] no product found for productId={}", enrichedOrder.productId)
                        enrichedOrder
                    } else {
                        enrichedOrder.copy(
                            productName = product.name,
                            productCategory = product.category
                        ).also { log.info("[JOIN2] KStream-GlobalKTable enriched orderId={} with product={}", enrichedOrder.orderId, product.name) }
                    }
                }
            )
            .to("20.enriched.products", Produced.with(Serdes.String(), jsonSerde<EnrichedOrder>()))
    }

    /**
     * JOIN 3: KStream-KStream windowed join
     *
     * Orders joined with Payments within a time window.
     * Both streams must be co-partitioned (same key=orderId).
     * JoinWindows defines how far apart events can be in time.
     *
     * Result: MatchedOrder written to 20.matched.orders
     */
    private fun buildKStreamKStreamWindowedJoin(builder: StreamsBuilder) {
        val ordersStream: KStream<String, OrderCreatedEvent> = builder
            .stream("20.orders.created", Consumed.with(Serdes.String(), jsonSerde<OrderCreatedEvent>()))

        val paymentsStream: KStream<String, PaymentProcessedEvent> = builder
            .stream("20.payments.processed", Consumed.with(Serdes.String(), jsonSerde<PaymentProcessedEvent>()))

        // KStream-KStream windowed join: both streams keyed by orderId
        // JoinWindows.ofTimeDifferenceWithNoGrace(5 minutes): order and payment must arrive within 5 minutes
        ordersStream
            .join(
                paymentsStream,
                { order, payment ->
                    MatchedOrder(
                        orderId = order.orderId,
                        userId = order.userId,
                        totalAmount = order.totalAmount,
                        paymentStatus = payment.status,
                        orderTimestamp = order.timestamp,
                        paymentTimestamp = payment.timestamp
                    ).also { log.info("[JOIN3] KStream-KStream matched orderId={} paymentStatus={}", order.orderId, payment.status) }
                },
                // window: events within 5 minutes of each other are eligible for joining
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                StreamJoined.with(
                    Serdes.String(),
                    jsonSerde<OrderCreatedEvent>(),
                    jsonSerde<PaymentProcessedEvent>()
                )
            )
            .to("20.matched.orders", Produced.with(Serdes.String(), jsonSerde<MatchedOrder>()))
    }

    private inline fun <reified T> jsonSerde(): org.apache.kafka.common.serialization.Serde<T> {
        val mapper = objectMapper
        return Serdes.serdeFrom(
            { _, data -> mapper.writeValueAsBytes(data) },
            { _, bytes -> if (bytes == null) null else mapper.readValue<T>(bytes) }
        )
    }
}