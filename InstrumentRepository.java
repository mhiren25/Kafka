package com.example.instrument.repository;

import com.example.instrument.domain.Instrument;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface InstrumentRepository extends JpaRepository<Instrument, Long> {
    
    /**
     * Find instrument by business ID
     */
    Optional<Instrument> findByInstrumentId(String instrumentId);
    
    /**
     * Find instrument by business ID and version
     */
    Optional<Instrument> findByInstrumentIdAndVersion(String instrumentId, Long version);
    
    /**
     * Get the latest version for an instrument
     */
    @Query("SELECT MAX(i.version) FROM Instrument i WHERE i.instrumentId = :instrumentId")
    Long findLatestVersionByInstrumentId(@Param("instrumentId") String instrumentId);
    
    /**
     * Check if an instrument exists with a specific or higher version
     */
    @Query("SELECT CASE WHEN COUNT(i) > 0 THEN true ELSE false END " +
           "FROM Instrument i WHERE i.instrumentId = :instrumentId AND i.version >= :version")
    boolean existsByInstrumentIdAndVersionGreaterThanEqual(
        @Param("instrumentId") String instrumentId, 
        @Param("version") Long version
    );
}
