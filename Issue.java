package com.example.instrument.domain;

import org.hibernate.annotations.DynamicUpdate;

import javax.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Issue table - stores all instrument types (Equity, Bond, Structured Product, Warrant)
 */
@Entity
@Table(name = "ISSUE", indexes = {
    @Index(name = "idx_issue_id_version", columnList = "instrumentId,version"),
    @Index(name = "idx_issue_id", columnList = "instrumentId"),
    @Index(name = "idx_issue_status", columnList = "status")
})
@DynamicUpdate
@EntityListeners(AuditListener.class)
public class Issue {
    
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "issue_seq")
    @SequenceGenerator(name = "issue_seq", sequenceName = "ISSUE_SEQ", allocationSize = 50)
    private Long id;
    
    @Column(nullable = false, length = 50)
    private String instrumentId;
    
    @Column(nullable = false)
    private Long version;
    
    @Column(nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    private InstrumentType instrumentType;
    
    @Column(nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    private RecordStatus status = RecordStatus.ACTIVE;
    
    // Common fields
    @Column(length = 200)
    private String name;
    
    @Column(length = 20)
    private String isin;
    
    @Column(length = 10)
    private String currency;
    
    @Column(length = 100)
    private String issuer;
    
    private LocalDate issueDate;
    
    private LocalDate maturityDate;
    
    @Column(precision = 19, scale = 2)
    private BigDecimal nominalValue;
    
    @Column(length = 50)
    private String country;
    
    // Metadata
    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;
    
    @Column(nullable = false)
    private LocalDateTime updatedAt;
    
    @Column(length = 50)
    private String correlationId;
    
    @Column(length = 100)
    private String lastUpdatedBy;
    
    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
        if (status == null) {
            status = RecordStatus.ACTIVE;
        }
    }
    
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
    
    @Version
    @Column(name = "opt_lock_version")
    private Long optLockVersion;
    
    // Constructors
    public Issue() {
    }
    
    // Getters and Setters
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public String getInstrumentId() {
        return instrumentId;
    }
    
    public void setInstrumentId(String instrumentId) {
        this.instrumentId = instrumentId;
    }
    
    public Long getVersion() {
        return version;
    }
    
    public void setVersion(Long version) {
        this.version = version;
    }
    
    public InstrumentType getInstrumentType() {
        return instrumentType;
    }
    
    public void setInstrumentType(InstrumentType instrumentType) {
        this.instrumentType = instrumentType;
    }
    
    public RecordStatus getStatus() {
        return status;
    }
    
    public void setStatus(RecordStatus status) {
        this.status = status;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getIsin() {
        return isin;
    }
    
    public void setIsin(String isin) {
        this.isin = isin;
    }
    
    public String getCurrency() {
        return currency;
    }
    
    public void setCurrency(String currency) {
        this.currency = currency;
    }
    
    public String getIssuer() {
        return issuer;
    }
    
    public void setIssuer(String issuer) {
        this.issuer = issuer;
    }
    
    public LocalDate getIssueDate() {
        return issueDate;
    }
    
    public void setIssueDate(LocalDate issueDate) {
        this.issueDate = issueDate;
    }
    
    public LocalDate getMaturityDate() {
        return maturityDate;
    }
    
    public void setMaturityDate(LocalDate maturityDate) {
        this.maturityDate = maturityDate;
    }
    
    public BigDecimal getNominalValue() {
        return nominalValue;
    }
    
    public void setNominalValue(BigDecimal nominalValue) {
        this.nominalValue = nominalValue;
    }
    
    public String getCountry() {
        return country;
    }
    
    public void setCountry(String country) {
        this.country = country;
    }
    
    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }
    
    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
    
    public String getCorrelationId() {
        return correlationId;
    }
    
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }
    
    public String getLastUpdatedBy() {
        return lastUpdatedBy;
    }
    
    public void setLastUpdatedBy(String lastUpdatedBy) {
        this.lastUpdatedBy = lastUpdatedBy;
    }
    
    public Long getOptLockVersion() {
        return optLockVersion;
    }
    
    public void setOptLockVersion(Long optLockVersion) {
        this.optLockVersion = optLockVersion;
    }
}

/**
 * Listing table - stores trading line information
 */
@Entity
@Table(name = "LISTING", indexes = {
    @Index(name = "idx_listing_id_version", columnList = "instrumentId,version"),
    @Index(name = "idx_listing_id", columnList = "instrumentId"),
    @Index(name = "idx_listing_exchange", columnList = "exchangeCode"),
    @Index(name = "idx_listing_status", columnList = "status")
})
@DynamicUpdate
@EntityListeners(AuditListener.class)
public class Listing {
    
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "listing_seq")
    @SequenceGenerator(name = "listing_seq", sequenceName = "LISTING_SEQ", allocationSize = 50)
    private Long id;
    
    @Column(nullable = false, length = 50)
    private String instrumentId;
    
    @Column(nullable = false)
    private Long version;
    
    @Column(nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    private InstrumentType instrumentType;
    
    @Column(nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    private RecordStatus status = RecordStatus.ACTIVE;
    
    // Trading line fields
    @Column(length = 20)
    private String exchangeCode;
    
    @Column(length = 50)
    private String tradingSymbol;
    
    @Column(precision = 19, scale = 6)
    private BigDecimal lastPrice;
    
    @Column(precision = 19, scale = 0)
    private BigDecimal volume;
    
    @Column(length = 20)
    private String tradingStatus;
    
    private LocalDate listingDate;
    
    @Column(precision = 5, scale = 2)
    private BigDecimal tickSize;
    
    @Column(length = 10)
    private String lotSize;
    
    @Column(length = 20)
    private String marketSegment;
    
    // Metadata
    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;
    
    @Column(nullable = false)
    private LocalDateTime updatedAt;
    
    @Column(length = 50)
    private String correlationId;
    
    @Column(length = 100)
    private String lastUpdatedBy;
    
    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
        if (status == null) {
            status = RecordStatus.ACTIVE;
        }
    }
    
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
    
    @Version
    @Column(name = "opt_lock_version")
    private Long optLockVersion;
    
    // Constructors
    public Listing() {
    }
    
    // Getters and Setters
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public String getInstrumentId() {
        return instrumentId;
    }
    
    public void setInstrumentId(String instrumentId) {
        this.instrumentId = instrumentId;
    }
    
    public Long getVersion() {
        return version;
    }
    
    public void setVersion(Long version) {
        this.version = version;
    }
    
    public InstrumentType getInstrumentType() {
        return instrumentType;
    }
    
    public void setInstrumentType(InstrumentType instrumentType) {
        this.instrumentType = instrumentType;
    }
    
    public RecordStatus getStatus() {
        return status;
    }
    
    public void setStatus(RecordStatus status) {
        this.status = status;
    }
    
    public String getExchangeCode() {
        return exchangeCode;
    }
    
    public void setExchangeCode(String exchangeCode) {
        this.exchangeCode = exchangeCode;
    }
    
    public String getTradingSymbol() {
        return tradingSymbol;
    }
    
    public void setTradingSymbol(String tradingSymbol) {
        this.tradingSymbol = tradingSymbol;
    }
    
    public BigDecimal getLastPrice() {
        return lastPrice;
    }
    
    public void setLastPrice(BigDecimal lastPrice) {
        this.lastPrice = lastPrice;
    }
    
    public BigDecimal getVolume() {
        return volume;
    }
    
    public void setVolume(BigDecimal volume) {
        this.volume = volume;
    }
    
    public String getTradingStatus() {
        return tradingStatus;
    }
    
    public void setTradingStatus(String tradingStatus) {
        this.tradingStatus = tradingStatus;
    }
    
    public LocalDate getListingDate() {
        return listingDate;
    }
    
    public void setListingDate(LocalDate listingDate) {
        this.listingDate = listingDate;
    }
    
    public BigDecimal getTickSize() {
        return tickSize;
    }
    
    public void setTickSize(BigDecimal tickSize) {
        this.tickSize = tickSize;
    }
    
    public String getLotSize() {
        return lotSize;
    }
    
    public void setLotSize(String lotSize) {
        this.lotSize = lotSize;
    }
    
    public String getMarketSegment() {
        return marketSegment;
    }
    
    public void setMarketSegment(String marketSegment) {
        this.marketSegment = marketSegment;
    }
    
    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }
    
    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
    
    public String getCorrelationId() {
        return correlationId;
    }
    
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }
    
    public String getLastUpdatedBy() {
        return lastUpdatedBy;
    }
    
    public void setLastUpdatedBy(String lastUpdatedBy) {
        this.lastUpdatedBy = lastUpdatedBy;
    }
    
    public Long getOptLockVersion() {
        return optLockVersion;
    }
    
    public void setOptLockVersion(Long optLockVersion) {
        this.optLockVersion = optLockVersion;
    }
}

/**
 * BondChar table - additional characteristics specific to bonds
 */
@Entity
@Table(name = "BOND_CHAR", indexes = {
    @Index(name = "idx_bond_char_id_version", columnList = "instrumentId,version"),
    @Index(name = "idx_bond_char_id", columnList = "instrumentId"),
    @Index(name = "idx_bond_char_status", columnList = "status")
})
@DynamicUpdate
@EntityListeners(AuditListener.class)
public class BondChar {
    
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "bond_char_seq")
    @SequenceGenerator(name = "bond_char_seq", sequenceName = "BOND_CHAR_SEQ", allocationSize = 50)
    private Long id;
    
    @Column(nullable = false, length = 50)
    private String instrumentId;
    
    @Column(nullable = false)
    private Long version;
    
    @Column(nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    private RecordStatus status = RecordStatus.ACTIVE;
    
    // Bond-specific characteristics
    @Column(precision = 5, scale = 2)
    private BigDecimal couponRate;
    
    @Column(length = 20)
    private String couponType;
    
    @Column(length = 20)
    private String couponFrequency;
    
    private LocalDate nextCouponDate;
    
    @Column(precision = 5, scale = 2)
    private BigDecimal yieldToMaturity;
    
    @Column(length = 20)
    private String ratingMoody;
    
    @Column(length = 20)
    private String ratingSP;
    
    @Column(length = 20)
    private String ratingFitch;
    
    @Column(length = 20)
    private String seniority;
    
    @Column(length = 10)
    private String callable;
    
    private LocalDate callDate;
    
    @Column(precision = 19, scale = 2)
    private BigDecimal callPrice;
    
    // Metadata
    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;
    
    @Column(nullable = false)
    private LocalDateTime updatedAt;
    
    @Column(length = 50)
    private String correlationId;
    
    @Column(length = 100)
    private String lastUpdatedBy;
    
    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
        if (status == null) {
            status = RecordStatus.ACTIVE;
        }
    }
    
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
    
    @Version
    @Column(name = "opt_lock_version")
    private Long optLockVersion;
    
    // Constructors
    public BondChar() {
    }
    
    // Getters and Setters
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public String getInstrumentId() {
        return instrumentId;
    }
    
    public void setInstrumentId(String instrumentId) {
        this.instrumentId = instrumentId;
    }
    
    public Long getVersion() {
        return version;
    }
    
    public void setVersion(Long version) {
        this.version = version;
    }
    
    public RecordStatus getStatus() {
        return status;
    }
    
    public void setStatus(RecordStatus status) {
        this.status = status;
    }
    
    public BigDecimal getCouponRate() {
        return couponRate;
    }
    
    public void setCouponRate(BigDecimal couponRate) {
        this.couponRate = couponRate;
    }
    
    public String getCouponType() {
        return couponType;
    }
    
    public void setCouponType(String couponType) {
        this.couponType = couponType;
    }
    
    public String getCouponFrequency() {
        return couponFrequency;
    }
    
    public void setCouponFrequency(String couponFrequency) {
        this.couponFrequency = couponFrequency;
    }
    
    public LocalDate getNextCouponDate() {
        return nextCouponDate;
    }
    
    public void setNextCouponDate(LocalDate nextCouponDate) {
        this.nextCouponDate = nextCouponDate;
    }
    
    public BigDecimal getYieldToMaturity() {
        return yieldToMaturity;
    }
    
    public void setYieldToMaturity(BigDecimal yieldToMaturity) {
        this.yieldToMaturity = yieldToMaturity;
    }
    
    public String getRatingMoody() {
        return ratingMoody;
    }
    
    public void setRatingMoody(String ratingMoody) {
        this.ratingMoody = ratingMoody;
    }
    
    public String getRatingSP() {
        return ratingSP;
    }
    
    public void setRatingSP(String ratingSP) {
        this.ratingSP = ratingSP;
    }
    
    public String getRatingFitch() {
        return ratingFitch;
    }
    
    public void setRatingFitch(String ratingFitch) {
        this.ratingFitch = ratingFitch;
    }
    
    public String getSeniority() {
        return seniority;
    }
    
    public void setSeniority(String seniority) {
        this.seniority = seniority;
    }
    
    public String getCallable() {
        return callable;
    }
    
    public void setCallable(String callable) {
        this.callable = callable;
    }
    
    public LocalDate getCallDate() {
        return callDate;
    }
    
    public void setCallDate(LocalDate callDate) {
        this.callDate = callDate;
    }
    
    public BigDecimal getCallPrice() {
        return callPrice;
    }
    
    public void setCallPrice(BigDecimal callPrice) {
        this.callPrice = callPrice;
    }
    
    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
    
    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }
    
    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
    
    public String getCorrelationId() {
        return correlationId;
    }
    
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }
    
    public String getLastUpdatedBy() {
        return lastUpdatedBy;
    }
    
    public void setLastUpdatedBy(String lastUpdatedBy) {
        this.lastUpdatedBy = lastUpdatedBy;
    }
    
    public Long getOptLockVersion() {
        return optLockVersion;
    }
    
    public void setOptLockVersion(Long optLockVersion) {
        this.optLockVersion = optLockVersion;
    }
}

/**
 * Instrument type enum
 */
enum InstrumentType {
    EQUITY,
    BOND,
    STRUCTURED_PRODUCT,
    WARRANT
}

/**
 * Record status enum for soft delete
 */
enum RecordStatus {
    ACTIVE,
    DELETED
}
