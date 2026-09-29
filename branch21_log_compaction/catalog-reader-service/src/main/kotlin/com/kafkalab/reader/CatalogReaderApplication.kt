package com.kafkalab.reader

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.kafka.annotation.EnableKafka

@SpringBootApplication
@EnableKafka
class CatalogReaderApplication

fun main(args: Array<String>) {
    runApplication<CatalogReaderApplication>(*args)
}