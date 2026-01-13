package com.example.instrument.domain;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.hibernate.annotations.DynamicUpdate;

import javax.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "INSTRUMENTS", indexes = {
    @Index(name = "idx_instrument_id_version", columnList = "instrumentId,version"),
    @Index(name = "idx_instrument_id", columnList = "instrumentId")
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@DynamicUpdate
public class Instrument {
    
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "instrument_seq")
    @SequenceGenerator(name = "instrument_seq", sequenceName = "INSTRUMENT_SEQ", allocationSize = 50)
    private Long id;
    
    @Column(nullable = false, length = 50)
    private String instrumentId;
    
    @Column(nullable = false)
    private Long version;
    
    // Main topic fields
    @Column(length = 100)
    private String name;
    
    @Column(length = 20)
    private String type;
    
    @Column(length = 10)
    private String currency;
    
    // Supplementary topic fields
    @Column(precision = 19, scale = 6)
    private BigDecimal price;
    
    @Column(precision = 19, scale = 6)
    private BigDecimal quantity;
    
    @Column(length = 50)
    private String issuer;
    
    @Column(length = 20)
    private String sector;
    
    @Column(length = 20)
    private String riskRating;
    
    private LocalDateTime maturityDate;
    
    @Column(precision = 5, scale = 2)
    private BigDecimal couponRate;
    
    @Column(length = 10)
    private String market;
    
    // Metadata
    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;
    
    @Column(nullable = false)
    private LocalDateTime updatedAt;
    
    @Column(length = 50)
    private String correlationId;
    
    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }
    
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
    
    @Version
    @Column(name = "opt_lock_version")
    private Long optLockVersion; // Optimistic locking
}
