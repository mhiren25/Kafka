package com.example.instrument.service;

import com.example.instrument.dto.AggregatedInstrumentUpdate;
import com.example.instrument.dto.InstrumentUpdateMessage;
import com.example.instrument.dto.PendingAggregation;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.LocalDateTime;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

@Service
@RequiredArgsConstructor
@Slf4j
public class InstrumentAggregationService {

    private final RestFallbackService restFallbackService;
    private final InstrumentProcessingQueue processingQueue;
    private final MeterRegistry meterRegistry;

    @Value("${instrument.aggregation.wait-window-ms}")
    private long waitWindowMs;

    @Value("${instrument.aggregation.max-pending-size}")
    private int maxPendingSize;

    @Value("${instrument.aggregation.stale-threshold-ms}")
    private long staleThresholdMs;

    // Map: instrumentId_version -> PendingAggregation
    private final ConcurrentHashMap<String, PendingAggregation> pendingAggregations = 
        new ConcurrentHashMap<>();
    
    // Scheduled executor for aggregation deadline processing
    private ScheduledExecutorService scheduledExecutor;
    
    // Locks per instrument to prevent race conditions during aggregation
    private final ConcurrentHashMap<String, ReentrantLock> aggregationLocks = 
        new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        scheduledExecutor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "aggregation-scheduler");
            t.setDaemon(true);
            return t;
        });
        
        meterRegistry.gauge("instrument.aggregation.pending.size", 
            pendingAggregations, ConcurrentHashMap::size);
    }

    @PreDestroy
    public void destroy() {
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdown();
            try {
                if (!scheduledExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduledExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduledExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public void processMessage(InstrumentUpdateMessage message) {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        String key = buildKey(message.getInstrumentId(), message.getVersion());
        ReentrantLock lock = aggregationLocks.computeIfAbsent(key, k -> new ReentrantLock());
        
        lock.lock();
        try {
            MDC.put("correlationId", message.getCorrelationId());
            MDC.put("instrumentId", message.getInstrumentId());
            MDC.put("version", String.valueOf(message.getVersion()));
            
            log.debug("Processing message from source: {}", message.getSource());
            
            // Get or create pending aggregation
            PendingAggregation pending = pendingAggregations.computeIfAbsent(key, k -> 
                createPendingAggregation(message)
            );
            
            // Add message to pending aggregation based on source
            addMessageToPending(pending, message);
            
            // Check if aggregation is complete or deadline reached
            if (pending.isComplete()) {
                log.debug("Aggregation complete for instrument {} version {}", 
                    message.getInstrumentId(), message.getVersion());
                completeAggregation(key, pending, false);
            } else if (pending.isMainReceived() && pending.isDeadlinePassed()) {
                log.debug("Aggregation deadline reached for instrument {} version {}", 
                    message.getInstrumentId(), message.getVersion());
                scheduleAggregationCompletion(key, pending);
            } else {
                log.debug("Waiting for more messages: {}/{} received", 
                    pending.getReceivedCount(), 9);
            }
            
            sample.stop(meterRegistry.timer("instrument.aggregation.process.time",
                "source", message.getSource()));
            
        } catch (Exception e) {
            log.error("Error processing message", e);
            meterRegistry.counter("instrument.aggregation.errors",
                "source", message.getSource()).increment();
            throw e;
        } finally {
            lock.unlock();
            MDC.clear();
        }
    }

    private PendingAggregation createPendingAggregation(InstrumentUpdateMessage message) {
        if (pendingAggregations.size() >= maxPendingSize) {
            log.warn("Pending aggregations size limit reached: {}", maxPendingSize);
            meterRegistry.counter("instrument.aggregation.size.limit.reached").increment();
        }
        
        LocalDateTime now = LocalDateTime.now();
        return PendingAggregation.builder()
            .instrumentId(message.getInstrumentId())
            .version(message.getVersion())
            .correlationId(message.getCorrelationId())
            .firstMessageAt(now)
            .aggregationDeadline(now.plusNanos(waitWindowMs * 1_000_000))
            .build();
    }

    private void addMessageToPending(PendingAggregation pending, InstrumentUpdateMessage message) {
        switch (message.getSource()) {
            case "main":
                pending.setMainMessage(message);
                break;
            case "pricing":
                pending.setPricingMessage(message);
                break;
            case "quantity":
                pending.setQuantityMessage(message);
                break;
            case "issuer":
                pending.setIssuerMessage(message);
                break;
            case "sector":
                pending.setSectorMessage(message);
                break;
            case "risk":
                pending.setRiskMessage(message);
                break;
            case "maturity":
                pending.setMaturityMessage(message);
                break;
            case "coupon":
                pending.setCouponMessage(message);
                break;
            case "market":
                pending.setMarketMessage(message);
                break;
            default:
                log.warn("Unknown source: {}", message.getSource());
        }
    }

    private void scheduleAggregationCompletion(String key, PendingAggregation pending) {
        long delayMs = Math.max(0, 
            java.time.Duration.between(LocalDateTime.now(), pending.getAggregationDeadline())
                .toMillis());
        
        scheduledExecutor.schedule(() -> {
            ReentrantLock lock = aggregationLocks.get(key);
            if (lock != null) {
                lock.lock();
                try {
                    PendingAggregation current = pendingAggregations.get(key);
                    if (current != null && !current.isComplete()) {
                        completeAggregation(key, current, true);
                    }
                } finally {
                    lock.unlock();
                }
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private void completeAggregation(String key, PendingAggregation pending, boolean useFallback) {
        try {
            AggregatedInstrumentUpdate aggregated;
            
            if (useFallback) {
                log.info("Using REST fallback for missing data");
                aggregated = restFallbackService.enrichWithRestData(pending);
                meterRegistry.counter("instrument.aggregation.rest.fallback.used").increment();
            } else {
                aggregated = buildAggregatedUpdate(pending, false);
            }
            
            // Submit to processing queue
            processingQueue.submit(aggregated);
            
            // Remove from pending
            pendingAggregations.remove(key);
            aggregationLocks.remove(key);
            
            meterRegistry.counter("instrument.aggregation.completed",
                "fallback", String.valueOf(useFallback)).increment();
            
            log.info("Aggregation completed for instrument {} version {}", 
                pending.getInstrumentId(), pending.getVersion());
            
        } catch (Exception e) {
            log.error("Error completing aggregation", e);
            meterRegistry.counter("instrument.aggregation.completion.errors").increment();
            throw e;
        }
    }

    private AggregatedInstrumentUpdate buildAggregatedUpdate(
            PendingAggregation pending, boolean fetchedFromRest) {
        
        InstrumentUpdateMessage main = pending.getMainMessage();
        
        return AggregatedInstrumentUpdate.builder()
            .instrumentId(pending.getInstrumentId())
            .version(pending.getVersion())
            .correlationId(pending.getCorrelationId())
            .receivedAt(pending.getFirstMessageAt())
            .name(main.getName())
            .type(main.getType())
            .currency(main.getCurrency())
            .price(pending.getPricingMessage() != null ? 
                pending.getPricingMessage().getPrice() : null)
            .quantity(pending.getQuantityMessage() != null ? 
                pending.getQuantityMessage().getQuantity() : null)
            .issuer(pending.getIssuerMessage() != null ? 
                pending.getIssuerMessage().getIssuer() : null)
            .sector(pending.getSectorMessage() != null ? 
                pending.getSectorMessage().getSector() : null)
            .riskRating(pending.getRiskMessage() != null ? 
                pending.getRiskMessage().getRiskRating() : null)
            .maturityDate(pending.getMaturityMessage() != null ? 
                pending.getMaturityMessage().getMaturityDate() : null)
            .couponRate(pending.getCouponMessage() != null ? 
                pending.getCouponMessage().getCouponRate() : null)
            .market(pending.getMarketMessage() != null ? 
                pending.getMarketMessage().getMarket() : null)
            .fetchedFromRest(fetchedFromRest)
            .sourceCount(pending.getReceivedCount())
            .build();
    }

    // Cleanup stale pending aggregations
    @Scheduled(fixedDelayString = "${instrument.aggregation.cleanup-interval-ms}")
    public void cleanupStalePending() {
        LocalDateTime threshold = LocalDateTime.now().minusNanos(staleThresholdMs * 1_000_000);
        
        pendingAggregations.entrySet().removeIf(entry -> {
            if (entry.getValue().getFirstMessageAt().isBefore(threshold)) {
                log.warn("Removing stale pending aggregation: {}", entry.getKey());
                aggregationLocks.remove(entry.getKey());
                meterRegistry.counter("instrument.aggregation.stale.removed").increment();
                return true;
            }
            return false;
        });
    }

    private String buildKey(String instrumentId, Long version) {
        return instrumentId + "_" + version;
    }
}
