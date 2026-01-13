package com.example.instrument.service;

import com.example.instrument.dto.AggregatedInstrumentUpdate;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.util.retry.Retry;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class InstrumentReloadService {

    private final WebClient webClient;
    private final MeterRegistry meterRegistry;

    @Value("${instrument.rest.base-url}")
    private String baseUrl;

    @Value("${instrument.rest.timeout-ms}")
    private long timeoutMs;

    @Value("${instrument.rest.max-retries}")
    private int maxRetries;

    @Value("${instrument.rest.retry-backoff-ms}")
    private long retryBackoffMs;

    @Value("${instrument.rest.endpoints.main}")
    private String mainEndpoint;

    @Value("${instrument.rest.endpoints.pricing}")
    private String pricingEndpoint;

    @Value("${instrument.rest.endpoints.quantity}")
    private String quantityEndpoint;

    @Value("${instrument.rest.endpoints.issuer}")
    private String issuerEndpoint;

    @Value("${instrument.rest.endpoints.sector}")
    private String sectorEndpoint;

    @Value("${instrument.rest.endpoints.risk}")
    private String riskEndpoint;

    @Value("${instrument.rest.endpoints.maturity}")
    private String maturityEndpoint;

    @Value("${instrument.rest.endpoints.coupon}")
    private String couponEndpoint;

    @Value("${instrument.rest.endpoints.market}")
    private String marketEndpoint;

    public AggregatedInstrumentUpdate reloadInstrument(
            String instrumentId, Long version, String correlationId) {
        
        log.info("Reloading complete instrument data for: {}", instrumentId);
        
        AggregatedInstrumentUpdate.AggregatedInstrumentUpdateBuilder builder = 
            AggregatedInstrumentUpdate.builder()
                .instrumentId(instrumentId)
                .correlationId(correlationId)
                .receivedAt(LocalDateTime.now())
                .fetchedFromRest(true)
                .sourceCount(9);
        
        // Fetch main data
        Map<String, Object> mainData = fetchData(instrumentId, mainEndpoint, "main");
        if (mainData != null) {
            builder.name((String) mainData.get("name"))
                   .type((String) mainData.get("type"))
                   .currency((String) mainData.get("currency"));
            
            // Use version from API if not provided
            if (version == null && mainData.get("version") != null) {
                version = ((Number) mainData.get("version")).longValue();
            }
        }
        
        builder.version(version != null ? version : System.currentTimeMillis());
        
        // Fetch supplementary data
        Map<String, Object> pricingData = fetchData(instrumentId, pricingEndpoint, "pricing");
        if (pricingData != null && pricingData.get("price") != null) {
            builder.price(new BigDecimal(pricingData.get("price").toString()));
        }
        
        Map<String, Object> quantityData = fetchData(instrumentId, quantityEndpoint, "quantity");
        if (quantityData != null && quantityData.get("quantity") != null) {
            builder.quantity(new BigDecimal(quantityData.get("quantity").toString()));
        }
        
        Map<String, Object> issuerData = fetchData(instrumentId, issuerEndpoint, "issuer");
        if (issuerData != null) {
            builder.issuer((String) issuerData.get("issuer"));
        }
        
        Map<String, Object> sectorData = fetchData(instrumentId, sectorEndpoint, "sector");
        if (sectorData != null) {
            builder.sector((String) sectorData.get("sector"));
        }
        
        Map<String, Object> riskData = fetchData(instrumentId, riskEndpoint, "risk");
        if (riskData != null) {
            builder.riskRating((String) riskData.get("riskRating"));
        }
        
        Map<String, Object> maturityData = fetchData(instrumentId, maturityEndpoint, "maturity");
        if (maturityData != null && maturityData.get("maturityDate") != null) {
            builder.maturityDate(LocalDateTime.parse(maturityData.get("maturityDate").toString()));
        }
        
        Map<String, Object> couponData = fetchData(instrumentId, couponEndpoint, "coupon");
        if (couponData != null && couponData.get("couponRate") != null) {
            builder.couponRate(new BigDecimal(couponData.get("couponRate").toString()));
        }
        
        Map<String, Object> marketData = fetchData(instrumentId, marketEndpoint, "market");
        if (marketData != null) {
            builder.market((String) marketData.get("market"));
        }
        
        AggregatedInstrumentUpdate update = builder.build();
        
        log.info("Successfully reloaded instrument: id={}, version={}", 
            instrumentId, update.getVersion());
        
        return update;
    }

    private Map<String, Object> fetchData(String instrumentId, String endpoint, String dataType) {
        try {
            String url = endpoint.replace("{instrumentId}", instrumentId);
            
            Map<String, Object> response = webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(Map.class)
                .timeout(Duration.ofMillis(timeoutMs))
                .retryWhen(Retry.backoff(maxRetries, Duration.ofMillis(retryBackoffMs))
                    .filter(throwable -> throwable instanceof org.springframework.web.reactive.function.client.WebClientResponseException &&
                        ((org.springframework.web.reactive.function.client.WebClientResponseException) throwable)
                            .getStatusCode().is5xxServerError())
                    .doBeforeRetry(signal -> 
                        log.warn("Retrying REST call for {} - attempt {}", 
                            dataType, signal.totalRetries() + 1)))
                .block();
            
            meterRegistry.counter("instrument.reload.endpoint.success",
                "endpoint", dataType).increment();
            
            return response;
            
        } catch (Exception e) {
            log.error("Error fetching {} data for instrument {}", dataType, instrumentId, e);
            meterRegistry.counter("instrument.reload.endpoint.errors",
                "endpoint", dataType).increment();
            return null;
        }
    }
}
