package com.example.instrument.listener;

import com.example.instrument.dto.RawMessage;
import com.example.instrument.service.PreTransformQueue;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Single Kafka listener that handles ALL instrument topics
 * Uses topic name from ConsumerRecord metadata to determine message type
 */
@Component
public class SingleInstrumentKafkaListener {
    
    private static final Logger log = LoggerFactory.getLogger(SingleInstrumentKafkaListener.class);
    
    private final PreTransformQueue preTransformQueue;
    private final MeterRegistry meterRegistry;

    public SingleInstrumentKafkaListener(
            PreTransformQueue preTransformQueue,
            MeterRegistry meterRegistry) {
        this.preTransformQueue = preTransformQueue;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Single listener for all 8 instrument topics
     * Topics: equity.issue, equity.trading-line, bond.issue, bond.trading-line,
     *         structured-product.issue, structured-product.trading-line,
     *         warrant.issue, warrant.trading-line
     */
    @KafkaListener(
        topics = {
            "${instrument.topics.equity-issue}",
            "${instrument.topics.equity-trading-line}",
            "${instrument.topics.bond-issue}",
            "${instrument.topics.bond-trading-line}",
            "${instrument.topics.structured-product-issue}",
            "${instrument.topics.structured-product-trading-line}",
            "${instrument.topics.warrant-issue}",
            "${instrument.topics.warrant-trading-line}"
        },
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        long startTime = System.currentTimeMillis();
        
        try {
            // Extract metadata from ConsumerRecord
            String topic = record.topic();
            String xmlContent = record.value();
            String key = record.key();
            long offset = record.offset();
            int partition = record.partition();
            
            log.debug("Received XML message: topic={}, partition={}, offset={}, size={} bytes", 
                topic, partition, offset, xmlContent != null ? xmlContent.length() : 0);
            
            // Create raw message
            RawMessage rawMessage = new RawMessage(xmlContent, topic, offset, partition, key);
            
            // Submit to pre-transform queue
            // This is a blocking operation with timeout if queue is full
            boolean submitted = preTransformQueue.submit(rawMessage);
            
            if (submitted) {
                // Acknowledge Kafka immediately after successful queue submission
                acknowledgment.acknowledge();
                
                long duration = System.currentTimeMillis() - startTime;
                
                meterRegistry.timer("kafka.message.ingestion.time",
                    "topic", topic
                ).record(duration, java.util.concurrent.TimeUnit.MILLISECONDS);
                
                meterRegistry.counter("kafka.message.received",
                    "topic", topic
                ).increment();
                
                log.debug("Message queued successfully: topic={}, duration={}ms", topic, duration);
            } else {
                // Queue rejected the message (should not happen with blocking queue)
                log.error("Failed to submit message to pre-transform queue: topic={}, offset={}", 
                    topic, offset);
                
                meterRegistry.counter("kafka.message.queue.rejected",
                    "topic", topic
                ).increment();
                
                // Don't acknowledge - will be redelivered
                throw new RuntimeException("Pre-transform queue rejected message");
            }
            
        } catch (Exception e) {
            log.error("Error processing Kafka message: topic={}, partition={}, offset={}", 
                record.topic(), record.partition(), record.offset(), e);
            
            meterRegistry.counter("kafka.message.ingestion.errors",
                "topic", record.topic(),
                "error", e.getClass().getSimpleName()
            ).increment();
            
            // Don't acknowledge - message will be redelivered
            throw e;
        }
    }
}
