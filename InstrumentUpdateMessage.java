package com.example.instrument.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class InstrumentUpdateMessage {
    private String instrumentId;
    private Long version;
    private String source; // Topic identifier
    private LocalDateTime timestamp;
    private String correlationId;
    
    // Main fields
    private String name;
    private String type;
    private String currency;
    
    // Supplementary fields
    private BigDecimal price;
    private BigDecimal quantity;
    private String issuer;
    private String sector;
    private String riskRating;
    private LocalDateTime maturityDate;
    private BigDecimal couponRate;
    private String market;
}

// Aggregated update ready for processing
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
class AggregatedInstrumentUpdate {
    private String instrumentId;
    private Long version;
    private String correlationId;
    private LocalDateTime receivedAt;
    
    // Aggregated data from all sources
    private String name;
    private String type;
    private String currency;
    private BigDecimal price;
    private BigDecimal quantity;
    private String issuer;
    private String sector;
    private String riskRating;
    private LocalDateTime maturityDate;
    private BigDecimal couponRate;
    private String market;
    
    // Metadata
    private boolean fetchedFromRest;
    private int sourceCount;
}

// Pending aggregation state
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
class PendingAggregation {
    private String instrumentId;
    private Long version;
    private String correlationId;
    private LocalDateTime firstMessageAt;
    private LocalDateTime aggregationDeadline;
    
    // Collected messages by source
    private InstrumentUpdateMessage mainMessage;
    private InstrumentUpdateMessage pricingMessage;
    private InstrumentUpdateMessage quantityMessage;
    private InstrumentUpdateMessage issuerMessage;
    private InstrumentUpdateMessage sectorMessage;
    private InstrumentUpdateMessage riskMessage;
    private InstrumentUpdateMessage maturityMessage;
    private InstrumentUpdateMessage couponMessage;
    private InstrumentUpdateMessage marketMessage;
    
    public boolean isMainReceived() {
        return mainMessage != null;
    }
    
    public boolean isComplete() {
        return mainMessage != null &&
               pricingMessage != null &&
               quantityMessage != null &&
               issuerMessage != null &&
               sectorMessage != null &&
               riskMessage != null &&
               maturityMessage != null &&
               couponMessage != null &&
               marketMessage != null;
    }
    
    public int getReceivedCount() {
        int count = 0;
        if (mainMessage != null) count++;
        if (pricingMessage != null) count++;
        if (quantityMessage != null) count++;
        if (issuerMessage != null) count++;
        if (sectorMessage != null) count++;
        if (riskMessage != null) count++;
        if (maturityMessage != null) count++;
        if (couponMessage != null) count++;
        if (marketMessage != null) count++;
        return count;
    }
    
    public boolean isDeadlinePassed() {
        return LocalDateTime.now().isAfter(aggregationDeadline);
    }
}
