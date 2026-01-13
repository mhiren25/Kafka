package com.example.instrument.config;

import com.example.instrument.dto.InstrumentUpdateMessage;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerConfig {

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

    @Bean
    public ConsumerFactory<String, InstrumentUpdateMessage> consumerFactory() {
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
        
        // Key deserializer
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        
        // Value deserializer with error handling
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, InstrumentUpdateMessage.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.instrument.*");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, InstrumentUpdateMessage> 
            kafkaListenerContainerFactory(ConsumerFactory<String, InstrumentUpdateMessage> consumerFactory) {
        
        ConcurrentKafkaListenerContainerFactory<String, InstrumentUpdateMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setPollTimeout(3000);
        
        // Error handling with exponential backoff
        factory.setCommonErrorHandler(errorHandler());
        
        // Consumer rebalance listener for metrics
        factory.getContainerProperties().setConsumerRebalanceListener(
            new MetricsConsumerRebalanceListener(meterRegistry)
        );
        
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
                // After all retries exhausted
                log.error("Retry exhausted for record: topic={}, partition={}, offset={}", 
                    record.topic(), record.partition(), record.offset(), exception);
                
                // Emit Prometheus alert metric
                meterRegistry.counter("kafka.consumer.retry.exhausted",
                    "topic", record.topic(),
                    "partition", String.valueOf(record.partition())
                ).increment();
                
                // Record could be sent to DLQ here if configured
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
