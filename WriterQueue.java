package com.example.instrument.service;

import com.example.instrument.dto.TransformedMessage;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Partitioned writer queue that ensures sequential processing per instrument
 * Each partition has its own queue and dedicated worker thread
 */
@Component
public class WriterQueue {
    
    private static final Logger log = LoggerFactory.getLogger(WriterQueue.class);
    
    private final InstrumentPersistenceService persistenceService;
    private final MeterRegistry meterRegistry;
    
    @Value("${instrument.writer-queue.partition-count:8}")
    private int partitionCount;
    
    @Value("${instrument.writer-queue.capacity-per-partition:5000}")
    private int capacityPerPartition;
    
    @Value("${instrument.writer-queue.offer-timeout-ms:5000}")
    private long offerTimeoutMs;
    
    @Value("${instrument.writer-queue.shutdown-timeout-seconds:60}")
    private int shutdownTimeoutSeconds;
    
    private List<BlockingQueue<TransformedMessage>> partitions;
    private ExecutorService executorService;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public WriterQueue(
            InstrumentPersistenceService persistenceService,
            MeterRegistry meterRegistry) {
        this.persistenceService = persistenceService;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void init() {
        log.info("Initializing writer queue with {} partitions, capacity {} per partition", 
            partitionCount, capacityPerPartition);
        
        // Create partitions
        partitions = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            BlockingQueue<TransformedMessage> queue = 
                new LinkedBlockingQueue<>(capacityPerPartition);
            partitions.add(queue);
            
            // Register metrics per partition
            final int partitionIndex = i;
            meterRegistry.gauge("instrument.writer_queue.size",
                "partition", String.valueOf(partitionIndex),
                queue, BlockingQueue::size);
        }
        
        // Create thread pool - one thread per partition for sequential processing
        executorService = Executors.newFixedThreadPool(partitionCount, r -> {
            Thread t = new Thread(r);
            t.setName("writer-" + t.getId());
            t.setDaemon(false);
            return t;
        });
        
        running.set(true);
        
        // Start worker threads
        for (int i = 0; i < partitionCount; i++) {
            final int partitionIndex = i;
            executorService.submit(() -> writerWorker(partitionIndex));
        }
        
        log.info("Writer queue initialized successfully");
    }

    /**
     * Submit transformed message to appropriate partition based on instrumentId
     */
    public void submit(TransformedMessage message) {
        if (!running.get()) {
            throw new IllegalStateException("Writer queue is not running");
        }
        
        int partition = getPartition(message.getInstrumentId());
        BlockingQueue<TransformedMessage> queue = partitions.get(partition);
        
        try {
            boolean added = queue.offer(message, offerTimeoutMs, TimeUnit.MILLISECONDS);
            if (added) {
                log.debug("Submitted to writer queue partition {}: instrument={}, version={}", 
                    partition, message.getInstrumentId(), message.getVersion());
                
                meterRegistry.counter("instrument.writer_queue.submitted",
                    "partition", String.valueOf(partition)).increment();
            } else {
                log.error("Failed to submit to writer queue partition {}: queue full", partition);
                meterRegistry.counter("instrument.writer_queue.rejected",
                    "partition", String.valueOf(partition)).increment();
                throw new RejectedExecutionException("Writer queue partition " + 
                    partition + " is full");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while submitting to writer queue", e);
            meterRegistry.counter("instrument.writer_queue.interrupted",
                "partition", String.valueOf(partition)).increment();
            throw new RuntimeException("Failed to submit to writer queue", e);
        }
    }

    /**
     * Worker thread that processes messages from a specific partition
     */
    private void writerWorker(int partitionIndex) {
        String workerName = Thread.currentThread().getName();
        log.info("Started writer worker for partition {}: {}", partitionIndex, workerName);
        
        BlockingQueue<TransformedMessage> queue = partitions.get(partitionIndex);
        
        while (running.get() || !queue.isEmpty()) {
            try {
                TransformedMessage message = queue.poll(1, TimeUnit.SECONDS);
                
                if (message != null) {
                    processMessage(message, partitionIndex);
                }
                
            } catch (InterruptedException e) {
                log.warn("Writer worker for partition {} interrupted", partitionIndex);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Unexpected error in writer worker for partition {}", 
                    partitionIndex, e);
                meterRegistry.counter("instrument.writer_queue.worker.errors",
                    "partition", String.valueOf(partitionIndex)).increment();
            }
        }
        
        log.info("Writer worker for partition {} stopped", partitionIndex);
    }

    /**
     * Process transformed message and persist to database
     */
    private void processMessage(TransformedMessage message, int partitionIndex) {
        long startTime = System.currentTimeMillis();
        
        try {
            log.debug("Processing in partition {}: instrument={}, version={}, type={}, category={}", 
                partitionIndex, message.getInstrumentId(), message.getVersion(),
                message.getInstrumentType(), message.getCategory());
            
            // Delegate to persistence service
            persistenceService.persistUpdate(message);
            
            long duration = System.currentTimeMillis() - startTime;
            meterRegistry.timer("instrument.writer.processing.duration",
                "partition", String.valueOf(partitionIndex),
                "instrument_type", message.getInstrumentType().name(),
                "category", message.getCategory().name()
            ).record(duration, TimeUnit.MILLISECONDS);
            
            meterRegistry.counter("instrument.writer.processing.success",
                "partition", String.valueOf(partitionIndex),
                "instrument_type", message.getInstrumentType().name(),
                "category", message.getCategory().name()
            ).increment();
            
            log.info("Successfully processed: instrument={}, version={}, type={}, duration={}ms", 
                message.getInstrumentId(), message.getVersion(), 
                message.getInstrumentType(), duration);
            
        } catch (InstrumentPersistenceService.StaleVersionException e) {
            // Expected - just log and continue
            log.debug("Stale version skipped: instrument={}, version={}", 
                message.getInstrumentId(), message.getVersion());
            
        } catch (Exception e) {
            log.error("Error processing message: instrument={}, version={}", 
                message.getInstrumentId(), message.getVersion(), e);
            
            meterRegistry.counter("instrument.writer.processing.errors",
                "partition", String.valueOf(partitionIndex),
                "error", e.getClass().getSimpleName()).increment();
            
            // TODO: Implement retry logic or DLQ
        }
    }

    /**
     * Get partition index for instrumentId using consistent hashing
     */
    private int getPartition(String instrumentId) {
        return Math.abs(instrumentId.hashCode() % partitionCount);
    }

    /**
     * Get total queue size across all partitions
     */
    public int getTotalSize() {
        return partitions.stream().mapToInt(BlockingQueue::size).sum();
    }

    /**
     * Get size of specific partition
     */
    public int getPartitionSize(int partition) {
        if (partition >= 0 && partition < partitionCount) {
            return partitions.get(partition).size();
        }
        return 0;
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down writer queue");
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
        
        // Log remaining items
        int totalRemaining = getTotalSize();
        if (totalRemaining > 0) {
            log.warn("Writer queue shutdown with {} items remaining", totalRemaining);
        }
        
        log.info("Writer queue shutdown complete");
    }
}
