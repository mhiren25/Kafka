package com.example.instrument.service;

import com.example.instrument.dto.RawMessage;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Simple FIFO queue for raw XML messages before transformation
 * This decouples Kafka consumption from XML transformation
 */
@Component
public class PreTransformQueue {
    
    private static final Logger log = LoggerFactory.getLogger(PreTransformQueue.class);
    
    private final MeterRegistry meterRegistry;
    
    @Value("${instrument.pre-transform-queue.capacity:50000}")
    private int queueCapacity;
    
    @Value("${instrument.pre-transform-queue.offer-timeout-ms:5000}")
    private long offerTimeoutMs;
    
    private BlockingQueue<RawMessage> queue;

    public PreTransformQueue(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void init() {
        queue = new LinkedBlockingQueue<>(queueCapacity);
        
        // Register gauge for queue size
        meterRegistry.gauge("instrument.pre_transform_queue.size", queue, BlockingQueue::size);
        
        log.info("Initialized pre-transform queue with capacity: {}", queueCapacity);
    }

    /**
     * Submit raw message to queue
     * Blocks with timeout if queue is full
     * 
     * @return true if submitted, false if rejected
     */
    public boolean submit(RawMessage rawMessage) {
        try {
            boolean offered = queue.offer(rawMessage, offerTimeoutMs, TimeUnit.MILLISECONDS);
            
            if (offered) {
                meterRegistry.counter("instrument.pre_transform_queue.submitted").increment();
            } else {
                log.error("Pre-transform queue full! Capacity: {}, Current size: {}", 
                    queueCapacity, queue.size());
                meterRegistry.counter("instrument.pre_transform_queue.rejected").increment();
            }
            
            return offered;
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while submitting to pre-transform queue", e);
            meterRegistry.counter("instrument.pre_transform_queue.interrupted").increment();
            return false;
        }
    }

    /**
     * Poll message from queue (used by transformer workers)
     * Blocks with timeout
     */
    public RawMessage poll(long timeout, TimeUnit unit) throws InterruptedException {
        RawMessage message = queue.poll(timeout, unit);
        
        if (message != null) {
            meterRegistry.counter("instrument.pre_transform_queue.polled").increment();
        }
        
        return message;
    }

    /**
     * Get current queue size
     */
    public int size() {
        return queue.size();
    }

    /**
     * Get remaining capacity
     */
    public int remainingCapacity() {
        return queue.remainingCapacity();
    }

    @PreDestroy
    public void destroy() {
        log.info("Shutting down pre-transform queue. Current size: {}", queue.size());
        
        if (!queue.isEmpty()) {
            log.warn("Pre-transform queue has {} unprocessed messages", queue.size());
        }
    }
}
