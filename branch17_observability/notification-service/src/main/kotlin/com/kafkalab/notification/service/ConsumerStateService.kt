package com.kafkalab.notification.service

import org.springframework.stereotype.Service

@Service
class ConsumerStateService {
    @Volatile var paused: Boolean = false
    @Volatile var processingDelayMs: Long = 0L
}