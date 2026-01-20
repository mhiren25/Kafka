package com.example.instrument.service;

import com.example.instrument.domain.InstrumentType;
import com.example.instrument.dto.MessageCategory;
import com.example.instrument.dto.RawMessage;
import com.example.instrument.dto.TransformedMessage;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Worker pool that transforms XML messages to vendor objects
 * Polls from pre-transform queue and submits to writer queue
 */
@Service
public class XmlTransformerWorkerPool {
    
    private static final Logger log = LoggerFactory.getLogger(XmlTransformerWorkerPool.class);
    
    private final PreTransformQueue preTransformQueue;
    private final WriterQueue writerQueue;
    private final XmlTransformerService xmlTransformerService;
    private final MeterRegistry meterRegistry;
    
    @Value("${instrument.transformer.worker-count:16}")
    private int workerCount;
    
    @Value("${instrument.transformer.poll-timeout-ms:1000}")
    private long pollTimeoutMs;
    
    @Value("${instrument.transformer.shutdown-timeout-seconds:30}")
    private int shutdownTimeoutSeconds;
    
    private ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public XmlTransformerWorkerPool(
            PreTransformQueue preTransformQueue,
            WriterQueue writerQueue,
            XmlTransformerService xmlTransformerService,
            MeterRegistry meterRegistry) {
        this.preTransformQueue = preTransformQueue;
        this.writerQueue = writerQueue;
        this.xmlTransformerService = xmlTransformerService;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void start() {
        log.info("Starting XML transformer worker pool with {} workers", workerCount);
        
        executorService = Executors.newFixedThreadPool(workerCount, r -> {
            Thread t = new Thread(r);
            t.setName("xml-transformer-" + t.getId());
            t.setDaemon(false);
            return t;
        });
        
        running.set(true);
        
        // Start worker threads
        for (int i = 0; i < workerCount; i++) {
            executorService.submit(this::transformerWorker);
        }
        
        log.info("XML transformer worker pool started successfully");
    }

    /**
     * Worker thread that continuously processes messages
     */
    private void transformerWorker() {
        String workerName = Thread.currentThread().getName();
        log.info("Transformer worker started: {}", workerName);
        
        while (running.get()) {
            try {
                // Poll from pre-transform queue
                RawMessage rawMessage = preTransformQueue.poll(pollTimeoutMs, TimeUnit.MILLISECONDS);
                
                if (rawMessage != null) {
                    processRawMessage(rawMessage);
                }
                
            } catch (InterruptedException e) {
                log.warn("Transformer worker interrupted: {}", workerName);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Unexpected error in transformer worker: {}", workerName, e);
                meterRegistry.counter("instrument.transformer.worker.errors",
                    "error", e.getClass().getSimpleName()).increment();
            }
        }
        
        log.info("Transformer worker stopped: {}", workerName);
    }

    /**
     * Process raw XML message
     */
    private void processRawMessage(RawMessage rawMessage) {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            String correlationId = UUID.randomUUID().toString();
            MDC.put("correlationId", correlationId);
            MDC.put("topic", rawMessage.getTopic());
            MDC.put("offset", String.valueOf(rawMessage.getOffset()));
            
            log.debug("Transforming XML from topic: {}", rawMessage.getTopic());
            
            // Determine message category from topic name
            MessageCategory category = determineCategory(rawMessage.getTopic());
            
            // Transform XML to vendor object
            Object vendorObject = transformXml(rawMessage.getXmlContent(), rawMessage.getTopic());
            
            // Determine instrument type using instanceof
            InstrumentType instrumentType = determineInstrumentType(vendorObject);
            
            // Extract metadata from vendor object
            String instrumentId = extractInstrumentId(vendorObject);
            Long version = extractVersion(vendorObject);
            
            // Build transformed message
            TransformedMessage transformedMessage = new TransformedMessage();
            transformedMessage.setVendorMessage(vendorObject);
            transformedMessage.setInstrumentType(instrumentType);
            transformedMessage.setCategory(category);
            transformedMessage.setInstrumentId(instrumentId);
            transformedMessage.setVersion(version);
            transformedMessage.setCorrelationId(correlationId);
            transformedMessage.setTopic(rawMessage.getTopic());
            transformedMessage.setOffset(rawMessage.getOffset());
            
            MDC.put("instrumentId", instrumentId);
            MDC.put("version", String.valueOf(version));
            MDC.put("instrumentType", instrumentType.name());
            MDC.put("category", category.name());
            
            log.debug("Transformed XML: instrumentId={}, version={}, type={}, category={}", 
                instrumentId, version, instrumentType, category);
            
            // Submit to writer queue
            writerQueue.submit(transformedMessage);
            
            sample.stop(meterRegistry.timer("instrument.transformation.duration",
                "instrument_type", instrumentType.name(),
                "category", category.name()));
            
            meterRegistry.counter("instrument.transformation.success",
                "instrument_type", instrumentType.name(),
                "category", category.name()).increment();
            
        } catch (XmlTransformerService.XmlTransformationException e) {
            log.error("XML transformation failed: topic={}, offset={}", 
                rawMessage.getTopic(), rawMessage.getOffset(), e);
            
            meterRegistry.counter("instrument.transformation.errors",
                "error", "transformation_failed",
                "topic", rawMessage.getTopic()).increment();
            
            // TODO: Send to DLQ or error handling
            
        } catch (Exception e) {
            log.error("Unexpected error during transformation: topic={}, offset={}", 
                rawMessage.getTopic(), rawMessage.getOffset(), e);
            
            meterRegistry.counter("instrument.transformation.errors",
                "error", e.getClass().getSimpleName(),
                "topic", rawMessage.getTopic()).increment();
            
        } finally {
            MDC.clear();
        }
    }

    /**
     * Determine message category from topic name
     */
    private MessageCategory determineCategory(String topic) {
        if (topic.contains("trading-line") || topic.contains("trading_line")) {
            return MessageCategory.TRADING_LINE;
        } else if (topic.contains("issue")) {
            return MessageCategory.ISSUE;
        } else {
            throw new IllegalArgumentException("Cannot determine category from topic: " + topic);
        }
    }

    /**
     * Transform XML using vendor transformer
     */
    private Object transformXml(String xmlContent, String topic) {
        // Determine which transformer to use based on topic
        if (topic.contains("equity")) {
            return xmlTransformerService.transformEquityXml(xmlContent);
        } else if (topic.contains("bond")) {
            return xmlTransformerService.transformBondXml(xmlContent);
        } else if (topic.contains("structured-product") || topic.contains("structured_product")) {
            return xmlTransformerService.transformStructuredProductXml(xmlContent);
        } else if (topic.contains("warrant")) {
            return xmlTransformerService.transformWarrantXml(xmlContent);
        } else {
            throw new IllegalArgumentException("Unknown topic type: " + topic);
        }
    }

    /**
     * Determine instrument type using instanceof
     */
    private InstrumentType determineInstrumentType(Object vendorObject) {
        if (vendorObject instanceof com.vendor.instruments.equity.EquityMessage) {
            return InstrumentType.EQUITY;
        } else if (vendorObject instanceof com.vendor.instruments.bond.BondMessage) {
            return InstrumentType.BOND;
        } else if (vendorObject instanceof com.vendor.instruments.structuredproduct.StructuredProductMessage) {
            return InstrumentType.STRUCTURED_PRODUCT;
        } else if (vendorObject instanceof com.vendor.instruments.warrant.WarrantMessage) {
            return InstrumentType.WARRANT;
        } else {
            throw new IllegalArgumentException("Unknown vendor message type: " + 
                vendorObject.getClass().getName());
        }
    }

    /**
     * Extract instrumentId from vendor object using reflection or casting
     */
    private String extractInstrumentId(Object vendorObject) {
        // All vendor messages should have getInstrumentId() method
        try {
            return (String) vendorObject.getClass().getMethod("getInstrumentId").invoke(vendorObject);
        } catch (Exception e) {
            log.error("Failed to extract instrumentId from vendor object", e);
            throw new RuntimeException("Cannot extract instrumentId", e);
        }
    }

    /**
     * Extract version from vendor object
     */
    private Long extractVersion(Object vendorObject) {
        try {
            return (Long) vendorObject.getClass().getMethod("getVersion").invoke(vendorObject);
        } catch (Exception e) {
            log.error("Failed to extract version from vendor object", e);
            throw new RuntimeException("Cannot extract version", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down XML transformer worker pool");
        running.set(false);
        
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(shutdownTimeoutSeconds, TimeUnit.SECONDS)) {
                    log.warn("Executor did not terminate gracefully, forcing shutdown");
                    executorService.shutdownNow();
                    
                    if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                        log.error("Executor did not terminate after force shutdown");
                    }
                }
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for executor termination", e);
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        log.info("XML transformer worker pool shutdown complete");
    }
}
