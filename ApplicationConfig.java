package com.example.instrument.config;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collection;

@Configuration
@EnableScheduling
@RequiredArgsConstructor
@Slf4j
public class ApplicationConfig {

    @Value("${instrument.rest.base-url}")
    private String restBaseUrl;

    @Bean
    public WebClient webClient() {
        return WebClient.builder()
            .baseUrl(restBaseUrl)
            .build();
    }
}

/**
 * Consumer rebalance listener for monitoring
 */
@Slf4j
@RequiredArgsConstructor
class MetricsConsumerRebalanceListener implements ConsumerRebalanceListener {

    private final MeterRegistry meterRegistry;

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Partitions revoked: {}", partitions);
        meterRegistry.counter("kafka.consumer.rebalance.revoked",
            "count", String.valueOf(partitions.size())).increment();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("Partitions assigned: {}", partitions);
        meterRegistry.counter("kafka.consumer.rebalance.assigned",
            "count", String.valueOf(partitions.size())).increment();
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        log.warn("Partitions lost: {}", partitions);
        meterRegistry.counter("kafka.consumer.rebalance.lost",
            "count", String.valueOf(partitions.size())).increment();
    }
}

// Exception Handler
package com.example.instrument.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@ControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleException(Exception e) {
        log.error("Unhandled exception", e);
        
        Map<String, Object> body = new HashMap<>();
        body.put("timestamp", LocalDateTime.now());
        body.put("status", HttpStatus.INTERNAL_SERVER_ERROR.value());
        body.put("error", "Internal Server Error");
        body.put("message", e.getMessage());
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body);
    }
}
