package com.example.instrument.service;

import com.example.instrument.domain.Instrument;
import com.example.instrument.dto.AggregatedInstrumentUpdate;
import com.example.instrument.repository.InstrumentRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.persistence.EntityManager;
import javax.persistence.LockModeType;
import javax.persistence.NoResultException;
import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class InstrumentPersistenceService {

    private final InstrumentRepository instrumentRepository;
    private final EntityManager entityManager;
    private final MeterRegistry meterRegistry;

    @Value("${instrument.version-validation.enabled}")
    private boolean versionValidationEnabled;

    @Value("${instrument.version-validation.log-skipped}")
    private boolean logSkipped;

    @Transactional
    public void persistUpdate(AggregatedInstrumentUpdate update) {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            MDC.put("correlationId", update.getCorrelationId());
            MDC.put("instrumentId", update.getInstrumentId());
            MDC.put("version", String.valueOf(update.getVersion()));
            
            log.debug("Starting persistence for instrument {} version {}", 
                update.getInstrumentId(), update.getVersion());
            
            // Register pre-commit hook for version validation
            if (versionValidationEnabled) {
                registerVersionValidationHook(update);
            }
            
            // Find existing instrument or create new
            Instrument instrument = instrumentRepository
                .findByInstrumentId(update.getInstrumentId())
                .orElse(null);
            
            if (instrument == null) {
                // New instrument - create
                instrument = createNewInstrument(update);
                instrumentRepository.save(instrument);
                
                log.info("Created new instrument: id={}, version={}", 
                    update.getInstrumentId(), update.getVersion());
                
                meterRegistry.counter("instrument.persistence.created").increment();
                
            } else {
                // Existing instrument - update
                // Lock row to prevent concurrent modifications
                entityManager.lock(instrument, LockModeType.PESSIMISTIC_WRITE);
                
                // Update fields
                updateInstrument(instrument, update);
                instrumentRepository.save(instrument);
                
                log.info("Updated instrument: id={}, version={}, prevVersion={}", 
                    update.getInstrumentId(), update.getVersion(), instrument.getVersion());
                
                meterRegistry.counter("instrument.persistence.updated").increment();
            }
            
            sample.stop(meterRegistry.timer("instrument.persistence.duration"));
            
        } catch (Exception e) {
            log.error("Error persisting instrument update", e);
            meterRegistry.counter("instrument.persistence.errors",
                "error", e.getClass().getSimpleName()).increment();
            throw e;
        } finally {
            MDC.clear();
        }
    }

    /**
     * Pre-commit validation hook that checks version before transaction commits
     */
    private void registerVersionValidationHook(AggregatedInstrumentUpdate update) {
        TransactionSynchronizationManager.registerSynchronization(
            new TransactionSynchronization() {
                @Override
                public void beforeCommit(boolean readOnly) {
                    if (!readOnly) {
                        validateVersionBeforeCommit(update);
                    }
                }
            }
        );
    }

    /**
     * Query the database for the latest version and skip if incoming version is stale
     */
    private void validateVersionBeforeCommit(AggregatedInstrumentUpdate update) {
        try {
            // Query for current persisted version
            Long currentVersion = getCurrentPersistedVersion(update.getInstrumentId());
            
            if (currentVersion != null && update.getVersion() <= currentVersion) {
                // Version is stale - skip persistence
                String message = String.format(
                    "Skipping stale update: instrument=%s, incomingVersion=%d, currentVersion=%d",
                    update.getInstrumentId(), update.getVersion(), currentVersion
                );
                
                if (logSkipped) {
                    log.warn(message);
                }
                
                meterRegistry.counter("instrument.persistence.skipped.stale",
                    "reason", "version_check").increment();
                
                // Mark transaction as rollback-only to prevent commit
                TransactionSynchronizationManager.setCurrentTransactionRollbackOnly();
                
                // Throw exception to rollback (will be caught and handled gracefully)
                throw new StaleVersionException(message);
            }
            
            log.debug("Version validation passed: incoming={}, current={}", 
                update.getVersion(), currentVersion);
            
        } catch (StaleVersionException e) {
            // Re-throw to trigger rollback
            throw e;
        } catch (Exception e) {
            log.error("Error during version validation", e);
            meterRegistry.counter("instrument.persistence.validation.errors").increment();
            // Don't block the commit on validation errors
        }
    }

    private Long getCurrentPersistedVersion(String instrumentId) {
        try {
            return entityManager
                .createQuery(
                    "SELECT i.version FROM Instrument i WHERE i.instrumentId = :instrumentId",
                    Long.class
                )
                .setParameter("instrumentId", instrumentId)
                .getSingleResult();
        } catch (NoResultException e) {
            return null; // Instrument doesn't exist yet
        }
    }

    private Instrument createNewInstrument(AggregatedInstrumentUpdate update) {
        return Instrument.builder()
            .instrumentId(update.getInstrumentId())
            .version(update.getVersion())
            .name(update.getName())
            .type(update.getType())
            .currency(update.getCurrency())
            .price(update.getPrice())
            .quantity(update.getQuantity())
            .issuer(update.getIssuer())
            .sector(update.getSector())
            .riskRating(update.getRiskRating())
            .maturityDate(update.getMaturityDate())
            .couponRate(update.getCouponRate())
            .market(update.getMarket())
            .correlationId(update.getCorrelationId())
            .createdAt(LocalDateTime.now())
            .updatedAt(LocalDateTime.now())
            .build();
    }

    private void updateInstrument(Instrument instrument, AggregatedInstrumentUpdate update) {
        instrument.setVersion(update.getVersion());
        instrument.setName(update.getName());
        instrument.setType(update.getType());
        instrument.setCurrency(update.getCurrency());
        instrument.setPrice(update.getPrice());
        instrument.setQuantity(update.getQuantity());
        instrument.setIssuer(update.getIssuer());
        instrument.setSector(update.getSector());
        instrument.setRiskRating(update.getRiskRating());
        instrument.setMaturityDate(update.getMaturityDate());
        instrument.setCouponRate(update.getCouponRate());
        instrument.setMarket(update.getMarket());
        instrument.setCorrelationId(update.getCorrelationId());
        instrument.setUpdatedAt(LocalDateTime.now());
    }

    /**
     * Custom exception for stale version detection
     */
    public static class StaleVersionException extends RuntimeException {
        public StaleVersionException(String message) {
            super(message);
        }
    }
}
