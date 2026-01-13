package com.example.instrument.service;

import com.example.instrument.dto.AggregatedInstrumentUpdate;
import com.example.instrument.dto.InstrumentUpdateMessage;
import com.example.instrument.dto.PendingAggregation;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class RestFallbackService {

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

    public AggregatedInstrumentUpdate enrichWithRestData(PendingAggregation pending) {
        String instrumentId = pending.getInstrumentId();
        Long version = pending.getVersion();
        
        log.info("Enriching instrument {} version {} with REST data", instrumentId, version);
        
        // Build base update from existing data
        AggregatedInstrumentUpdate.AggregatedInstrumentUpdateBuilder builder = 
            AggregatedInstrumentUpdate.builder()
                .instrumentId(instrumentId)
                .version(version)
                .correlationId(pending.getCorrelationId())
                .receivedAt(pending.getFirstMessageAt())
                .fetchedFromRest(true)
                .sourceCount(pending.getReceivedCount());
        
        // Copy main data
        if (pending.getMainMessage() != null) {
            InstrumentUpdateMessage main = pending.getMainMessage();
            builder.name(main.getName())
                   .type(main.getType())
                   .currency(main.getCurrency());
        }
        
        // Fetch missing supplementary data
        if (pending.getPricingMessage() == null) {
            fetchPrice(instrumentId, version).ifPresent(builder::price);
        } else {
            builder.price(pending.getPricingMessage().getPrice());
        }
        
        if (pending.getQuantityMessage() == null) {
            fetchQuantity(instrumentId, version).ifPresent(builder::quantity);
        } else {
            builder.quantity(pending.getQuantityMessage().getQuantity());
        }
        
        if (pending.getIssuerMessage() == null) {
            fetchIssuer(instrumentId, version).ifPresent(builder::issuer);
        } else {
            builder.issuer(pending.getIssuerMessage().getIssuer());
        }
        
        if (pending.getSectorMessage() == null) {
            fetchSector(instrumentId, version).ifPresent(builder::sector);
        } else {
            builder.sector(pending.getSectorMessage().getSector());
        }
        
        if (pending.getRiskMessage() == null) {
            fetchRiskRating(instrumentId, version).ifPresent(builder::riskRating);
        } else {
            builder.riskRating(pending.getRiskMessage().getRiskRating());
        }
        
        if (pending.getMaturityMessage() == null) {
            fetchMaturityDate(instrumentId, version).ifPresent(builder::maturityDate);
        } else {
            builder.maturityDate(pending.getMaturityMessage().getMaturityDate());
        }
        
        if (pending.getCouponMessage() == null) {
            fetchCouponRate(instrumentId, version).ifPresent(builder::couponRate);
        } else {
            builder.couponRate(pending.getCouponMessage().getCouponRate());
        }
        
        if (pending.getMarketMessage() == null) {
            fetchMarket(instrumentId, version).ifPresent(builder::market);
        } else {
            builder.market(pending.getMarketMessage().getMarket());
        }
        
        return builder.build();
    }

    private java.util.Optional<BigDecimal> fetchPrice(String instrumentId, Long version) {
        return fetchData(instrumentId, version, pricingEndpoint, "pricing", 
            response -> response.get("price") != null ? 
                new BigDecimal(response.get("price").toString()) : null);
    }

    private java.util.Optional<BigDecimal> fetchQuantity(String instrumentId, Long version) {
        return fetchData(instrumentId, version, quantityEndpoint, "quantity",
            response -> response.get("quantity") != null ? 
                new BigDecimal(response.get("quantity").toString()) : null);
    }

    private java.util.Optional<String> fetchIssuer(String instrumentId, Long version) {
        return fetchData(instrumentId, version, issuerEndpoint, "issuer",
            response -> (String) response.get("issuer"));
    }

    private java.util.Optional<String> fetchSector(String instrumentId, Long version) {
        return fetchData(instrumentId, version, sectorEndpoint, "sector",
            response -> (String) response.get("sector"));
    }

    private java.util.Optional<String> fetchRiskRating(String instrumentId, Long version) {
        return fetchData(instrumentId, version, riskEndpoint, "risk",
            response -> (String) response.get("riskRating"));
    }

    private java.util.Optional<LocalDateTime> fetchMaturityDate(String instrumentId, Long version) {
        return fetchData(instrumentId, version, maturityEndpoint, "maturity",
            response -> response.get("maturityDate") != null ? 
                LocalDateTime.parse(response.get("maturityDate").toString()) : null);
    }

    private java.util.Optional<BigDecimal> fetchCouponRate(String instrumentId, Long version) {
        return fetchData(instrumentId, version, couponEndpoint, "coupon",
            response -> response.get("couponRate") != null ? 
                new BigDecimal(response.get("couponRate").toString()) : null);
    }

    private java.util.Optional<String> fetchMarket(String instrumentId, Long version) {
        return fetchData(instrumentId, version, marketEndpoint, "market",
            response -> (String) response.get("market"));
    }

    private <T> java.util.Optional<T> fetchData(
            String instrumentId, 
            Long version,
            String endpoint, 
            String dataType,
            java.util.function.Function<Map<String, Object>, T> extractor) {
        
        try {
            String url = endpoint.replace("{instrumentId}", instrumentId);
            
            Map<String, Object> response = webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(Map.class)
                .timeout(Duration.ofMillis(timeoutMs))
                .retryWhen(Retry.backoff(maxRetries, Duration.ofMillis(retryBackoffMs))
                    .filter(throwable -> throwable instanceof WebClientResponseException &&
                        ((WebClientResponseException) throwable).getStatusCode().is5xxServerError())
                    .doBeforeRetry(signal -> {
                        log.warn("Retrying REST call for {} - attempt {}", 
                            dataType, signal.totalRetries() + 1);
                        meterRegistry.counter("instrument.rest.retry",
                            "endpoint", dataType).increment();
                    }))
                .onErrorResume(throwable -> {
                    log.error("Error fetching {} for instrument {}", dataType, instrumentId, throwable);
                    meterRegistry.counter("instrument.rest.errors",
                        "endpoint", dataType).increment();
                    return Mono.empty();
                })
                .block();
            
            if (response != null) {
                T data = extractor.apply(response);
                if (data != null) {
                    meterRegistry.counter("instrument.rest.success",
                        "endpoint", dataType).increment();
                    return java.util.Optional.of(data);
                }
            }
            
            meterRegistry.counter("instrument.rest.no.data",
                "endpoint", dataType).increment();
            return java.util.Optional.empty();
            
        } catch (Exception e) {
            log.error("Unexpected error fetching {} for instrument {}", 
                dataType, instrumentId, e);
            meterRegistry.counter("instrument.rest.errors",
                "endpoint", dataType).increment();
            return java.util.Optional.empty();
        }
    }
}
