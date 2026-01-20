package com.example.instrument.service;

import com.example.instrument.domain.*;
import com.example.instrument.dto.TransformedMessage;
import com.example.instrument.repository.BondCharRepository;
import com.example.instrument.repository.IssueRepository;
import com.example.instrument.repository.ListingRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class InstrumentPersistenceService {
    
    private static final Logger log = LoggerFactory.getLogger(InstrumentPersistenceService.class);

    private final IssueRepository issueRepository;
    private final ListingRepository listingRepository;
    private final BondCharRepository bondCharRepository;
    private final EntityManager entityManager;
    private final MeterRegistry meterRegistry;
    private final VendorMessageMapper vendorMessageMapper;

    @Value("${instrument.version-validation.enabled}")
    private boolean versionValidationEnabled;

    @Value("${instrument.version-validation.log-skipped}")
    private boolean logSkipped;

    public InstrumentPersistenceService(
            IssueRepository issueRepository,
            ListingRepository listingRepository,
            BondCharRepository bondCharRepository,
            EntityManager entityManager,
            MeterRegistry meterRegistry,
            VendorMessageMapper vendorMessageMapper) {
        this.issueRepository = issueRepository;
        this.listingRepository = listingRepository;
        this.bondCharRepository = bondCharRepository;
        this.entityManager = entityManager;
        this.meterRegistry = meterRegistry;
        this.vendorMessageMapper = vendorMessageMapper;
    }

    @Transactional
    public void persistUpdate(TransformedMessage transformedMessage) {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            MDC.put("correlationId", transformedMessage.getCorrelationId());
            MDC.put("instrumentId", transformedMessage.getInstrumentId());
            MDC.put("version", String.valueOf(transformedMessage.getVersion()));
            MDC.put("instrumentType", transformedMessage.getInstrumentType().name());
            MDC.put("category", transformedMessage.getCategory().name());
            
            log.debug("Starting persistence for instrument {} version {} type {} category {}", 
                transformedMessage.getInstrumentId(), 
                transformedMessage.getVersion(), 
                transformedMessage.getInstrumentType(),
                transformedMessage.getCategory());
            
            // Register pre-commit validation hooks
            if (versionValidationEnabled) {
                registerIssueVersionValidationHook(transformedMessage);
                
                if (transformedMessage.getCategory() == MessageCategory.TRADING_LINE) {
                    registerListingVersionValidationHook(transformedMessage);
                    
                    if (transformedMessage.getInstrumentType() == InstrumentType.BOND) {
                        registerBondCharVersionValidationHook(transformedMessage);
                    }
                }
            }
            
            // Process based on message category
            if (transformedMessage.getCategory() == MessageCategory.ISSUE) {
                // ISSUE message → Only persist Issue table
                persistIssue(transformedMessage);
                
            } else {
                // TRADING_LINE message → Persist Issue + Listing (+ BondChar for bonds)
                persistIssue(transformedMessage);
                
                // Check if listing data exists in message
                if (vendorMessageMapper.hasListingData(transformedMessage.getVendorMessage())) {
                    persistListing(transformedMessage);
                } else {
                    // No listing data → Soft delete existing listing
                    softDeleteListing(transformedMessage.getInstrumentId());
                }
                
                // For bonds, persist bond characteristics
                if (transformedMessage.getInstrumentType() == InstrumentType.BOND) {
                    if (vendorMessageMapper.hasBondCharData(transformedMessage.getVendorMessage())) {
                        persistBondChar(transformedMessage);
                    } else {
                        softDeleteBondChar(transformedMessage.getInstrumentId());
                    }
                }
            }
            
            sample.stop(meterRegistry.timer("instrument.persistence.duration",
                "instrument_type", transformedMessage.getInstrumentType().name(),
                "category", transformedMessage.getCategory().name()));
            
            log.info("Successfully persisted instrument {} version {} type {} category {}", 
                transformedMessage.getInstrumentId(), 
                transformedMessage.getVersion(), 
                transformedMessage.getInstrumentType(),
                transformedMessage.getCategory());
            
        } catch (StaleVersionException e) {
            log.debug("Stale version detected during persistence: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Error persisting instrument update", e);
            meterRegistry.counter("instrument.persistence.errors",
                "error", e.getClass().getSimpleName(),
                "instrument_type", transformedMessage.getInstrumentType().name()).increment();
            throw e;
        } finally {
            MDC.clear();
        }
    }

    private void persistIssue(TransformedMessage transformedMessage) {
        Issue issue = issueRepository
            .findByInstrumentIdAndStatus(transformedMessage.getInstrumentId(), RecordStatus.ACTIVE)
            .orElse(null);
        
        if (issue == null) {
            // Create new issue
            issue = vendorMessageMapper.mapToIssue(transformedMessage);
            issue.setStatus(RecordStatus.ACTIVE);
            issue.setLastUpdatedBy("KAFKA_" + transformedMessage.getCategory());
            issueRepository.save(issue);
            
            log.info("Created new issue: id={}, version={}, type={}", 
                transformedMessage.getInstrumentId(), 
                transformedMessage.getVersion(), 
                transformedMessage.getInstrumentType());
            
            meterRegistry.counter("instrument.persistence.issue.created",
                "instrument_type", transformedMessage.getInstrumentType().name()).increment();
        } else {
            // Update existing issue
            entityManager.lock(issue, LockModeType.PESSIMISTIC_WRITE);
            vendorMessageMapper.updateIssue(issue, transformedMessage);
            issue.setStatus(RecordStatus.ACTIVE);
            issue.setLastUpdatedBy("KAFKA_" + transformedMessage.getCategory());
            issueRepository.save(issue);
            
            log.info("Updated issue: id={}, version={}, type={}", 
                transformedMessage.getInstrumentId(), 
                transformedMessage.getVersion(), 
                transformedMessage.getInstrumentType());
            
            meterRegistry.counter("instrument.persistence.issue.updated",
                "instrument_type", transformedMessage.getInstrumentType().name()).increment();
        }
    }

    private void persistListing(TransformedMessage transformedMessage) {
        Listing listing = listingRepository
            .findByInstrumentIdAndStatus(transformedMessage.getInstrumentId(), RecordStatus.ACTIVE)
            .orElse(null);
        
        if (listing == null) {
            // Create new listing
            listing = vendorMessageMapper.mapToListing(transformedMessage);
            listing.setStatus(RecordStatus.ACTIVE);
            listing.setLastUpdatedBy("KAFKA_TRADING_LINE");
            listingRepository.save(listing);
            
            log.info("Created new listing: id={}, version={}, type={}", 
                transformedMessage.getInstrumentId(), 
                transformedMessage.getVersion(), 
                transformedMessage.getInstrumentType());
            
            meterRegistry.counter("instrument.persistence.listing.created",
                "instrument_type", transformedMessage.getInstrumentType().name()).increment();
        } else {
            // Update existing listing
            entityManager.lock(listing, LockModeType.PESSIMISTIC_WRITE);
            vendorMessageMapper.updateListing(listing, transformedMessage);
            listing.setStatus(RecordStatus.ACTIVE); // Ensure it's active
            listing.setLastUpdatedBy("KAFKA_TRADING_LINE");
            listingRepository.save(listing);
            
            log.info("Updated listing: id={}, version={}, type={}", 
                transformedMessage.getInstrumentId(), 
                transformedMessage.getVersion(), 
                transformedMessage.getInstrumentType());
            
            meterRegistry.counter("instrument.persistence.listing.updated",
                "instrument_type", transformedMessage.getInstrumentType().name()).increment();
        }
    }

    private void persistBondChar(TransformedMessage transformedMessage) {
        BondChar bondChar = bondCharRepository
            .findByInstrumentIdAndStatus(transformedMessage.getInstrumentId(), RecordStatus.ACTIVE)
            .orElse(null);
        
        if (bondChar == null) {
            // Create new bond characteristics
            bondChar = vendorMessageMapper.mapToBondChar(transformedMessage);
            bondChar.setStatus(RecordStatus.ACTIVE);
            bondChar.setLastUpdatedBy("KAFKA_TRADING_LINE");
            bondCharRepository.save(bondChar);
            
            log.info("Created new bond characteristics: id={}, version={}", 
                transformedMessage.getInstrumentId(), transformedMessage.getVersion());
            
            meterRegistry.counter("instrument.persistence.bond_char.created").increment();
        } else {
            // Update existing bond characteristics
            entityManager.lock(bondChar, LockModeType.PESSIMISTIC_WRITE);
            vendorMessageMapper.updateBondChar(bondChar, transformedMessage);
            bondChar.setStatus(RecordStatus.ACTIVE);
            bondChar.setLastUpdatedBy("KAFKA_TRADING_LINE");
            bondCharRepository.save(bondChar);
            
            log.info("Updated bond characteristics: id={}, version={}", 
                transformedMessage.getInstrumentId(), transformedMessage.getVersion());
            
            meterRegistry.counter("instrument.persistence.bond_char.updated").increment();
        }
    }

    /**
     * Soft delete listing - mark as DELETED instead of removing from database
     */
    private void softDeleteListing(String instrumentId) {
        Listing listing = listingRepository
            .findByInstrumentIdAndStatus(instrumentId, RecordStatus.ACTIVE)
            .orElse(null);
        
        if (listing != null) {
            entityManager.lock(listing, LockModeType.PESSIMISTIC_WRITE);
            listing.setStatus(RecordStatus.DELETED);
            listing.setUpdatedAt(LocalDateTime.now());
            listing.setLastUpdatedBy("KAFKA_SOFT_DELETE");
            listingRepository.save(listing);
            
            log.warn("Soft deleted listing: instrumentId={}, version={}", 
                instrumentId, listing.getVersion());
            
            meterRegistry.counter("instrument.persistence.listing.soft_deleted").increment();
        } else {
            log.debug("No active listing found to soft delete for instrumentId={}", instrumentId);
        }
    }

    /**
     * Soft delete bond characteristics
     */
    private void softDeleteBondChar(String instrumentId) {
        BondChar bondChar = bondCharRepository
            .findByInstrumentIdAndStatus(instrumentId, RecordStatus.ACTIVE)
            .orElse(null);
        
        if (bondChar != null) {
            entityManager.lock(bondChar, LockModeType.PESSIMISTIC_WRITE);
            bondChar.setStatus(RecordStatus.DELETED);
            bondChar.setUpdatedAt(LocalDateTime.now());
            bondChar.setLastUpdatedBy("KAFKA_SOFT_DELETE");
            bondCharRepository.save(bondChar);
            
            log.warn("Soft deleted bond char: instrumentId={}, version={}", 
                instrumentId, bondChar.getVersion());
            
            meterRegistry.counter("instrument.persistence.bond_char.soft_deleted").increment();
        } else {
            log.debug("No active bond char found to soft delete for instrumentId={}", instrumentId);
        }
    }

    // Version validation hooks (same as before)
    private void registerIssueVersionValidationHook(TransformedMessage transformedMessage) {
        TransactionSynchronizationManager.registerSynchronization(
            new TransactionSynchronization() {
                @Override
                public void beforeCommit(boolean readOnly) {
                    if (!readOnly) {
                        validateIssueVersionBeforeCommit(transformedMessage);
                    }
                }
            }
        );
    }

    private void registerListingVersionValidationHook(TransformedMessage transformedMessage) {
        TransactionSynchronizationManager.registerSynchronization(
            new TransactionSynchronization() {
                @Override
                public void beforeCommit(boolean readOnly) {
                    if (!readOnly) {
                        validateListingVersionBeforeCommit(transformedMessage);
                    }
                }
            }
        );
    }

    private void registerBondCharVersionValidationHook(TransformedMessage transformedMessage) {
        TransactionSynchronizationManager.registerSynchronization(
            new TransactionSynchronization() {
                @Override
                public void beforeCommit(boolean readOnly) {
                    if (!readOnly) {
                        validateBondCharVersionBeforeCommit(transformedMessage);
                    }
                }
            }
        );
    }

    private void validateIssueVersionBeforeCommit(TransformedMessage transformedMessage) {
        try {
            Long currentVersion = getCurrentIssueVersion(transformedMessage.getInstrumentId());
            
            if (currentVersion != null && transformedMessage.getVersion() <= currentVersion) {
                String message = String.format(
                    "Skipping stale issue update: instrument=%s, incomingVersion=%d, currentVersion=%d",
                    transformedMessage.getInstrumentId(), transformedMessage.getVersion(), currentVersion
                );
                
                if (logSkipped) {
                    log.warn(message);
                }
                
                meterRegistry.counter("instrument.persistence.skipped.stale.issue",
                    "instrument_type", transformedMessage.getInstrumentType().name()).increment();
                
                TransactionSynchronizationManager.setCurrentTransactionRollbackOnly();
                throw new StaleVersionException(message);
            }
            
        } catch (StaleVersionException e) {
            throw e;
        } catch (Exception e) {
            log.error("Error during issue version validation", e);
            meterRegistry.counter("instrument.persistence.validation.errors",
                "table", "issue").increment();
        }
    }

    private void validateListingVersionBeforeCommit(TransformedMessage transformedMessage) {
        try {
            Long currentVersion = getCurrentListingVersion(transformedMessage.getInstrumentId());
            
            if (currentVersion != null && transformedMessage.getVersion() <= currentVersion) {
                String message = String.format(
                    "Skipping stale listing update: instrument=%s, incomingVersion=%d, currentVersion=%d",
                    transformedMessage.getInstrumentId(), transformedMessage.getVersion(), currentVersion
                );
                
                if (logSkipped) {
                    log.warn(message);
                }
                
                meterRegistry.counter("instrument.persistence.skipped.stale.listing",
                    "instrument_type", transformedMessage.getInstrumentType().name()).increment();
                
                TransactionSynchronizationManager.setCurrentTransactionRollbackOnly();
                throw new StaleVersionException(message);
            }
            
        } catch (StaleVersionException e) {
            throw e;
        } catch (Exception e) {
            log.error("Error during listing version validation", e);
            meterRegistry.counter("instrument.persistence.validation.errors",
                "table", "listing").increment();
        }
    }

    private void validateBondCharVersionBeforeCommit(TransformedMessage transformedMessage) {
        try {
            Long currentVersion = getCurrentBondCharVersion(transformedMessage.getInstrumentId());
            
            if (currentVersion != null && transformedMessage.getVersion() <= currentVersion) {
                String message = String.format(
                    "Skipping stale bond char update: instrument=%s, incomingVersion=%d, currentVersion=%d",
                    transformedMessage.getInstrumentId(), transformedMessage.getVersion(), currentVersion
                );
                
                if (logSkipped) {
                    log.warn(message);
                }
                
                meterRegistry.counter("instrument.persistence.skipped.stale.bond_char").increment();
                
                TransactionSynchronizationManager.setCurrentTransactionRollbackOnly();
                throw new StaleVersionException(message);
            }
            
        } catch (StaleVersionException e) {
            throw e;
        } catch (Exception e) {
            log.error("Error during bond char version validation", e);
            meterRegistry.counter("instrument.persistence.validation.errors",
                "table", "bond_char").increment();
        }
    }

    private Long getCurrentIssueVersion(String instrumentId) {
        try {
            return entityManager
                .createQuery("SELECT i.version FROM Issue i WHERE i.instrumentId = :instrumentId AND i.status = :status", Long.class)
                .setParameter("instrumentId", instrumentId)
                .setParameter("status", RecordStatus.ACTIVE)
                .getSingleResult();
        } catch (NoResultException e) {
            return null;
        }
    }

    private Long getCurrentListingVersion(String instrumentId) {
        try {
            return entityManager
                .createQuery("SELECT l.version FROM Listing l WHERE l.instrumentId = :instrumentId AND l.status = :status", Long.class)
                .setParameter("instrumentId", instrumentId)
                .setParameter("status", RecordStatus.ACTIVE)
                .getSingleResult();
        } catch (NoResultException e) {
            return null;
        }
    }

    private Long getCurrentBondCharVersion(String instrumentId) {
        try {
            return entityManager
                .createQuery("SELECT b.version FROM BondChar b WHERE b.instrumentId = :instrumentId AND b.status = :status", Long.class)
                .setParameter("instrumentId", instrumentId)
                .setParameter("status", RecordStatus.ACTIVE)
                .getSingleResult();
        } catch (NoResultException e) {
            return null;
        }
    }

    public static class StaleVersionException extends RuntimeException {
        public StaleVersionException(String message) {
            super(message);
        }
    }
}
