package com.kafkalab.analytics.config

import com.kafkalab.avro.OrderCreatedEvent
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.WindowStore
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
class KafkaStreamsTopology(
    @Value("\${schema-registry.url}")
    private val schemaRegistryUrl: String
) {
    private val log = LoggerFactory.getLogger(javaClass)

    private fun orderSerde(): SpecificAvroSerde<OrderCreatedEvent> {
        val serde = SpecificAvroSerde<OrderCreatedEvent>()
        serde.configure(mapOf("schema.registry.url" to schemaRegistryUrl), false)
        return serde
    }

    @Bean
    fun ordersStream(streamsBuilder: StreamsBuilder): KStream<String, OrderCreatedEvent> {
        val orderSerde = orderSerde()

        val orders: KStream<String, OrderCreatedEvent> = streamsBuilder.stream(
            "14.orders.created",
            Consumed.with(Serdes.String(), orderSerde)
        )

        // ── 1. Rolling count per userId (KTable + state store) ───────────────
        // groupBy re-keys the stream by userId; count() produces a KTable
        orders
            .groupBy(
                { _, v -> v.userId.toString() },
                Grouped.with(Serdes.String(), orderSerde)
            )
            .count(
                Materialized.`as`<String, Long, KeyValueStore<Bytes, ByteArray>>("orders-by-user-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long())
            )
            .toStream()
            .peek { userId, count ->
                log.info("[STREAMS][count-by-user] userId={} totalOrders={}", userId, count)
            }
            .to("14.analytics.orders-by-user", Produced.with(Serdes.String(), Serdes.Long()))

        // ── 2. Tumbling window (1 min) — count per category ──────────────────
        // WindowedBy returns a TimeWindowedKStream; the key becomes Windowed<String>
        orders
            .groupBy(
                { _, v -> v.category.toString() },
                Grouped.with(Serdes.String(), orderSerde)
            )
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
            .count()
            .toStream()
            .peek { windowedKey, count ->
                log.info(
                    "[STREAMS][1min-tumbling] category={} window=[{} - {}] count={}",
                    windowedKey.key(),
                    windowedKey.window().startTime(),
                    windowedKey.window().endTime(),
                    count
                )
            }

        // ── 3. Sum of sales per userId in tumbling 5-min window ──────────────
        orders
            .groupBy(
                { _, v -> v.userId.toString() },
                Grouped.with(Serdes.String(), orderSerde)
            )
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
            .aggregate(
                { 0.0 },
                { _, v, agg -> agg + v.totalAmount },
                Materialized.`as`<String, Double, WindowStore<Bytes, ByteArray>>("sales-by-user-5min-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Double())
            )
            .toStream()
            .map { key: Windowed<String>, value: Double -> KeyValue(key.key(), value) }
            .peek { userId, total ->
                log.info("[STREAMS][5min-tumbling] userId={} sales={}", userId, total)
            }

        // ── 4. Filter high-value orders (>= 50.0) ────────────────────────────
        orders
            .filter { _, v -> v.totalAmount >= 50.0 }
            .peek { _, v ->
                log.info("[STREAMS][high-value] orderId={} category={} total={}", v.orderId, v.category, v.totalAmount)
            }
            .to("14.analytics.orders-high-value", Produced.with(Serdes.String(), orderSerde))

        return orders
    }
}