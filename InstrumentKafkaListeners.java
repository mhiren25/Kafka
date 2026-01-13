package com.example.instrument.listener;

import com.example.instrument.dto.InstrumentUpdateMessage;
import com.example.instrument.service.InstrumentAggregationService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class InstrumentKafkaListeners {

    private final InstrumentAggregationService aggregationService;
    private final MeterRegistry meterRegistry;

    @KafkaListener(
        topics = "${instrument.topics.main}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void listenMain(ConsumerRecord<String, InstrumentUpdateMessage> record, 
                          Acknowledgment acknowledgment) {
        processMessage(record, acknowledgment, "main");
    }

    @KafkaListener(
        topics = "#{${instrument.topics.supplementary}[0]}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void listenPricing(ConsumerRecord<String, InstrumentUpdateMessage> record,
                             Acknowledgment acknowledgment) {
        processMessage(record, acknowledgment, "pricing");
    }

    @KafkaListener(
        topics = "#{${instrument.topics.supplementary}[1]}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void listenQuantity(ConsumerRecord<String, InstrumentUpdateMessage> record,
                              Acknowledgment acknowledgment) {
        processMessage(record, acknowledgment, "quantity");
    }

    @KafkaListener(
        topics = "#{${instrument.topics.supplementary}[2]}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void listenIssuer(ConsumerRecord<String, InstrumentUpdateMessage> record,
                            Acknowledgment acknowledgment) {
        processMessage(record, acknowledgment, "issuer");
    }

    @KafkaListener(
        topics = "#{${instrument.topics.supplementary}[3]}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void listenSector(ConsumerRecord<String, InstrumentUpdateMessage> record,
                            Acknowledgment acknowledgment) {
        processMessage(record, acknowledgment, "sector");
    }

    @KafkaListener(
        topics = "#{${instrument.topics.supplementary}[4]}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void listenRisk(ConsumerRecord<String, InstrumentUpdateMessage> record,
                          Acknowledgment acknowledgment) {
        processMessage(record, acknowledgment, "risk");
    }

    @KafkaListener(
        topics = "#{${instrument.topics.supplementary}[5]}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void listenMaturity(ConsumerRecord<String, InstrumentUpdateMessage> record,
                              Acknowledgment acknowledgment) {
        processMessage(record, acknowledgment, "maturity");
    }

    @KafkaListener(
        topics = "#{${instrument.topics.supplementary}[6]}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void listenCoupon(ConsumerRecord<String, InstrumentUpdateMessage> record,
                            Acknowledgment acknowledgment) {
        processMessage(record, acknowledgment, "coupon");
    }

    @KafkaListener(
        topics = "#{${instrument.topics.supplementary}[7]}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void listenMarket(ConsumerRecord<String, InstrumentUpdateMessage> record,
                            Acknowledgment acknowledgment) {
        processMessage(record, acknowledgment, "market");
    }

    private void processMessage(ConsumerRecord<String, InstrumentUpdateMessage> record,
                               Acknowledgment acknowledgment,
                               String source) {
        long startTime = System.currentTimeMillis();
        InstrumentUpdateMessage message = record.value();
        
        // Set correlation ID if not present
        if (message.getCorrelationId() == null) {
            message.setCorrelationId(UUID.randomUUID().toString());
        }
        
        // Set source
        message.setSource(source);
        
        try {
            MDC.put("correlationId", message.getCorrelationId());
            MDC.put("instrumentId", message.getInstrumentId());
            MDC.put("version", String.valueOf(message.getVersion()));
            MDC.put("source", source);
            
            log.debug("Received message: topic={}, partition={}, offset={}, key={}", 
                record.topic(), record.partition(), record.offset(), record.key());
            
            // Process message through aggregation service
            aggregationService.processMessage(message);
            
            // Acknowledge only after successful processing
            acknowledgment.acknowledge();
            
            long duration = System.currentTimeMillis() - startTime;
            meterRegistry.timer("kafka.message.processing.time",
                "topic", record.topic(),
                "source", source
            ).record(duration, java.util.concurrent.TimeUnit.MILLISECONDS);
            
            meterRegistry.counter("kafka.message.received",
                "topic", record.topic(),
                "source", source
            ).increment();
            
            log.debug("Message processed successfully: source={}, duration={}ms", 
                source, duration);
            
        } catch (Exception e) {
            log.error("Error processing message from source: {}", source, e);
            
            meterRegistry.counter("kafka.message.processing.errors",
                "topic", record.topic(),
                "source", source,
                "error", e.getClass().getSimpleName()
            ).increment();
            
            // Don't acknowledge - message will be redelivered
            throw e;
            
        } finally {
            MDC.clear();
        }
    }
}
