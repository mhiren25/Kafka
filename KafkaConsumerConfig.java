package com.example.instrument.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka configuration for consuming XML messages as Strings
 * Each topic receives XML which will be transformed using vendor transformers
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);
    
    private final MeterRegistry meterRegistry;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;

    @Value("${spring.kafka.retry.backoff.initial-interval}")
    private Long retryInitialInterval;

    @Value("${spring.kafka.retry.backoff.max-interval}")
    private Long retryMaxInterval;

    @Value("${spring.kafka.retry.backoff.multiplier}")
    private Double retryMultiplier;

    @Value("${spring.kafka.retry.max-attempts}")
    private Integer maxAttempts;

    public KafkaConsumerConfig(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Base consumer factory configuration for XML (String) messages
     * All topics receive XML strings that need to be transformed
     */
    @Bean
    public ConsumerFactory<String, String> xmlConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);
        props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 540000);
        
        // Both key and value are strings (XML content)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Single container factory for all XML-based topics
     * The transformation from XML to domain objects happens in the listener
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> xmlConsumerFactory) {
        
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(xmlConsumerFactory);
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.setCommonErrorHandler(errorHandler());
        factory.getContainerProperties().setConsumerRebalanceListener(
            new MetricsConsumerRebalanceListener(meterRegistry)
        );
        
        log.info("Initialized Kafka listener container factory for XML messages");
        
        return factory;
    }

    @Bean
    public CommonErrorHandler errorHandler() {
        ExponentialBackOff backOff = new ExponentialBackOff(
            retryInitialInterval,
            retryMultiplier
        );
        backOff.setMaxInterval(retryMaxInterval);
        
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            (record, exception) -> {
                log.error("Retry exhausted for record: topic={}, partition={}, offset={}", 
                    record.topic(), record.partition(), record.offset(), exception);
                
                meterRegistry.counter("kafka.consumer.retry.exhausted",
                    "topic", record.topic(),
                    "partition", String.valueOf(record.partition())
                ).increment();
            },
            backOff
        );
        
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.warn("Retry attempt {} for record: topic={}, partition={}, offset={}", 
                deliveryAttempt, record.topic(), record.partition(), record.offset());
            
            meterRegistry.counter("kafka.consumer.retry.attempts",
                "topic", record.topic(),
                "attempt", String.valueOf(deliveryAttempt)
            ).increment();
        });
        
        return errorHandler;
    }
}
