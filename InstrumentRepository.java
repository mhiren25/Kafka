package com.example.instrument.repository;

import com.example.instrument.domain.BondChar;
import com.example.instrument.domain.Issue;
import com.example.instrument.domain.Listing;
import com.example.instrument.domain.RecordStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface IssueRepository extends JpaRepository<Issue, Long> {
    
    /**
     * Find active issue by instrumentId
     */
    Optional<Issue> findByInstrumentIdAndStatus(String instrumentId, RecordStatus status);
    
    /**
     * Find by instrumentId and version (regardless of status)
     */
    Optional<Issue> findByInstrumentIdAndVersion(String instrumentId, Long version);
    
    /**
     * Get latest version for active issue
     */
    @Query("SELECT MAX(i.version) FROM Issue i WHERE i.instrumentId = :instrumentId AND i.status = 'ACTIVE'")
    Long findLatestVersionByInstrumentId(@Param("instrumentId") String instrumentId);
    
    /**
     * Check if active issue exists with version >= specified
     */
    @Query("SELECT CASE WHEN COUNT(i) > 0 THEN true ELSE false END " +
           "FROM Issue i WHERE i.instrumentId = :instrumentId AND i.version >= :version AND i.status = 'ACTIVE'")
    boolean existsByInstrumentIdAndVersionGreaterThanEqual(
        @Param("instrumentId") String instrumentId, 
        @Param("version") Long version
    );
}

@Repository
interface ListingRepository extends JpaRepository<Listing, Long> {
    
    /**
     * Find active listing by instrumentId
     */
    Optional<Listing> findByInstrumentIdAndStatus(String instrumentId, RecordStatus status);
    
    /**
     * Find by instrumentId and version (regardless of status)
     */
    Optional<Listing> findByInstrumentIdAndVersion(String instrumentId, Long version);
    
    /**
     * Get latest version for active listing
     */
    @Query("SELECT MAX(l.version) FROM Listing l WHERE l.instrumentId = :instrumentId AND l.status = 'ACTIVE'")
    Long findLatestVersionByInstrumentId(@Param("instrumentId") String instrumentId);
    
    /**
     * Check if active listing exists with version >= specified
     */
    @Query("SELECT CASE WHEN COUNT(l) > 0 THEN true ELSE false END " +
           "FROM Listing l WHERE l.instrumentId = :instrumentId AND l.version >= :version AND l.status = 'ACTIVE'")
    boolean existsByInstrumentIdAndVersionGreaterThanEqual(
        @Param("instrumentId") String instrumentId, 
        @Param("version") Long version
    );
}

@Repository
interface BondCharRepository extends JpaRepository<BondChar, Long> {
    
    /**
     * Find active bond char by instrumentId
     */
    Optional<BondChar> findByInstrumentIdAndStatus(String instrumentId, RecordStatus status);
    
    /**
     * Find by instrumentId and version (regardless of status)
     */
    Optional<BondChar> findByInstrumentIdAndVersion(String instrumentId, Long version);
    
    /**
     * Get latest version for active bond char
     */
    @Query("SELECT MAX(b.version) FROM BondChar b WHERE b.instrumentId = :instrumentId AND b.status = 'ACTIVE'")
    Long findLatestVersionByInstrumentId(@Param("instrumentId") String instrumentId);
    
    /**
     * Check if active bond char exists with version >= specified
     */
    @Query("SELECT CASE WHEN COUNT(b) > 0 THEN true ELSE false END " +
           "FROM BondChar b WHERE b.instrumentId = :instrumentId AND b.version >= :version AND b.status = 'ACTIVE'")
    boolean existsByInstrumentIdAndVersionGreaterThanEqual(
        @Param("instrumentId") String instrumentId, 
        @Param("version") Long version
    );
}
