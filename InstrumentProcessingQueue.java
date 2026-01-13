package com.example.instrument.service;

import com.example.instrument.dto.AggregatedInstrumentUpdate;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Partitioned queue that ensures:
 * - Same instrumentId always goes to same partition (same thread)
 * - Sequential processing per instrument
 * - Parallel processing across different instruments
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class InstrumentProcessingQueue {

    private final InstrumentPersistenceService persistenceService;
    private final MeterRegistry meterRegistry;

    @Value("${instrument.processing.partition-count}")
    private int partitionCount;

    @Value("${instrument.processing.queue-capacity}")
    private int queueCapacity;

    @Value("${instrument.processing.thread-pool-size}")
    private int threadPoolSize;

    @Value("${instrument.processing.thread-name-prefix}")
    private String threadNamePrefix;

    @Value("${instrument.processing.await-termination-seconds}")
    private int awaitTerminationSeconds;

    private List<BlockingQueue<AggregatedInstrumentUpdate>> partitions;
    private ExecutorService executorService;
    private volatile boolean running = false;

    @PostConstruct
    public void init() {
        log.info("Initializing processing queue with {} partitions", partitionCount);
        
        // Create partitions
        partitions = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            BlockingQueue<AggregatedInstrumentUpdate> queue = 
                new LinkedBlockingQueue<>(queueCapacity / partitionCount);
            partitions.add(queue);
            
            // Register metrics per partition
            int partitionIndex = i;
            meterRegistry.gauge("instrument.processing.queue.size",
                "partition", String.valueOf(partitionIndex),
                queue, BlockingQueue::size);
        }
        
        // Create thread pool - one thread per partition for sequential processing
        executorService = Executors.newFixedThreadPool(threadPoolSize, r -> {
            Thread t = new Thread(r);
            t.setName(threadNamePrefix + t.getId());
            t.setDaemon(false);
            return t;
        });
        
        // Start worker threads
        running = true;
        for (int i = 0; i < partitionCount; i++) {
            final int partitionIndex = i;
            executorService.submit(() -> processPartition(partitionIndex));
        }
        
        log.info("Processing queue initialized successfully");
    }

    @PreDestroy
    public void destroy() {
        log.info("Shutting down processing queue");
        running = false;
        
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(awaitTerminationSeconds, TimeUnit.SECONDS)) {
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
        int totalRemaining = partitions.stream().mapToInt(BlockingQueue::size).sum();
        if (totalRemaining > 0) {
            log.warn("Processing queue shutdown with {} items remaining", totalRemaining);
        }
        
        log.info("Processing queue shutdown complete");
    }

    public void submit(AggregatedInstrumentUpdate update) {
        if (!running) {
            throw new IllegalStateException("Processing queue is not running");
        }
        
        int partition = getPartition(update.getInstrumentId());
        BlockingQueue<AggregatedInstrumentUpdate> queue = partitions.get(partition);
        
        try {
            boolean added = queue.offer(update, 5, TimeUnit.SECONDS);
            if (added) {
                log.debug("Submitted update to partition {}: instrument={}, version={}", 
                    partition, update.getInstrumentId(), update.getVersion());
                
                meterRegistry.counter("instrument.processing.queue.submitted",
                    "partition", String.valueOf(partition)).increment();
            } else {
                log.error("Failed to submit update to partition {}: queue full", partition);
                meterRegistry.counter("instrument.processing.queue.rejected",
                    "partition", String.valueOf(partition),
                    "reason", "queue_full").increment();
                throw new RejectedExecutionException("Processing queue partition " + 
                    partition + " is full");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while submitting to queue", e);
            meterRegistry.counter("instrument.processing.queue.rejected",
                "partition", String.valueOf(partition),
                "reason", "interrupted").increment();
            throw new RuntimeException("Failed to submit update", e);
        }
    }

    private void processPartition(int partitionIndex) {
        log.info("Started processing thread for partition {}", partitionIndex);
        BlockingQueue<AggregatedInstrumentUpdate> queue = partitions.get(partitionIndex);
        
        while (running || !queue.isEmpty()) {
            try {
                AggregatedInstrumentUpdate update = queue.poll(1, TimeUnit.SECONDS);
                
                if (update != null) {
                    processUpdate(update, partitionIndex);
                }
                
            } catch (InterruptedException e) {
                log.warn("Processing thread for partition {} interrupted", partitionIndex);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Unexpected error in processing thread for partition {}", 
                    partitionIndex, e);
                meterRegistry.counter("instrument.processing.thread.errors",
                    "partition", String.valueOf(partitionIndex)).increment();
            }
        }
        
        log.info("Processing thread for partition {} stopped", partitionIndex);
    }

    private void processUpdate(AggregatedInstrumentUpdate update, int partitionIndex) {
        long startTime = System.currentTimeMillis();
        
        try {
            log.debug("Processing update in partition {}: instrument={}, version={}", 
                partitionIndex, update.getInstrumentId(), update.getVersion());
            
            // Delegate to persistence service
            persistenceService.persistUpdate(update);
            
            long duration = System.currentTimeMillis() - startTime;
            meterRegistry.timer("instrument.processing.duration",
                "partition", String.valueOf(partitionIndex)).record(duration, TimeUnit.MILLISECONDS);
            
            meterRegistry.counter("instrument.processing.success",
                "partition", String.valueOf(partitionIndex)).increment();
            
            log.info("Successfully processed update: instrument={}, version={}, duration={}ms", 
                update.getInstrumentId(), update.getVersion(), duration);
            
        } catch (Exception e) {
            log.error("Error processing update: instrument={}, version={}", 
                update.getInstrumentId(), update.getVersion(), e);
            
            meterRegistry.counter("instrument.processing.errors",
                "partition", String.valueOf(partitionIndex),
                "error", e.getClass().getSimpleName()).increment();
            
            // Could implement retry logic or DLQ here
            throw e;
        }
    }

    private int getPartition(String instrumentId) {
        // Use consistent hashing to ensure same instrument always goes to same partition
        return Math.abs(instrumentId.hashCode() % partitionCount);
    }

    public int getQueueSize() {
        return partitions.stream().mapToInt(BlockingQueue::size).sum();
    }

    public int getPartitionSize(int partition) {
        if (partition >= 0 && partition < partitionCount) {
            return partitions.get(partition).size();
        }
        return 0;
    }
}
