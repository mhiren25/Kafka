package com.example.instrument.controller;

import com.example.instrument.dto.AggregatedInstrumentUpdate;
import com.example.instrument.service.InstrumentProcessingQueue;
import com.example.instrument.service.InstrumentReloadService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/instruments")
@RequiredArgsConstructor
@Slf4j
public class InstrumentReloadController {

    private final InstrumentReloadService reloadService;
    private final InstrumentProcessingQueue processingQueue;
    private final MeterRegistry meterRegistry;

    /**
     * Reload full instrument snapshot from external REST APIs
     */
    @PostMapping("/{instrumentId}/reload")
    public ResponseEntity<ReloadResponse> reloadInstrument(
            @PathVariable String instrumentId,
            @RequestParam(required = false) Long version,
            @RequestHeader(value = "X-Correlation-ID", required = false) String correlationId) {
        
        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
        }
        
        MDC.put("correlationId", correlationId);
        MDC.put("instrumentId", instrumentId);
        
        try {
            log.info("Received reload request for instrument: {}, version: {}", 
                instrumentId, version);
            
            meterRegistry.counter("instrument.reload.requests").increment();
            
            // Fetch complete instrument data from REST APIs
            AggregatedInstrumentUpdate update = reloadService.reloadInstrument(
                instrumentId, version, correlationId);
            
            // Submit to processing queue (same path as Kafka messages)
            processingQueue.submit(update);
            
            meterRegistry.counter("instrument.reload.success").increment();
            
            log.info("Reload request submitted successfully: instrument={}, version={}", 
                instrumentId, update.getVersion());
            
            return ResponseEntity.accepted().body(ReloadResponse.builder()
                .instrumentId(instrumentId)
                .version(update.getVersion())
                .correlationId(correlationId)
                .status("ACCEPTED")
                .message("Reload request submitted for processing")
                .build());
            
        } catch (Exception e) {
            log.error("Error processing reload request", e);
            
            meterRegistry.counter("instrument.reload.errors",
                "error", e.getClass().getSimpleName()).increment();
            
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ReloadResponse.builder()
                    .instrumentId(instrumentId)
                    .correlationId(correlationId)
                    .status("ERROR")
                    .message("Failed to reload instrument: " + e.getMessage())
                    .build());
            
        } finally {
            MDC.clear();
        }
    }

    /**
     * Health check endpoint
     */
    @GetMapping("/health")
    public ResponseEntity<HealthResponse> health() {
        int queueSize = processingQueue.getQueueSize();
        
        return ResponseEntity.ok(HealthResponse.builder()
            .status("UP")
            .queueSize(queueSize)
            .build());
    }

    @Data
    @lombok.Builder
    public static class ReloadResponse {
        private String instrumentId;
        private Long version;
        private String correlationId;
        private String status;
        private String message;
    }

    @Data
    @lombok.Builder
    public static class HealthResponse {
        private String status;
        private int queueSize;
    }
}
