package com.kafkalab.order.controller

import com.kafkalab.order.model.BenchmarkResult
import com.kafkalab.order.service.BenchmarkService
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/benchmark")
class BenchmarkController(private val benchmarkService: BenchmarkService) {

    @PostMapping("/run")
    fun run(
        @RequestParam mode: String,
        @RequestParam(defaultValue = "100") count: Int
    ): BenchmarkResult = benchmarkService.runBenchmark(mode, count)

    @PostMapping("/compare")
    fun compare(
        @RequestParam(defaultValue = "100") count: Int
    ): Map<String, BenchmarkResult> = benchmarkService.compareAll(count)
}