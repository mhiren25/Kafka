package com.example.instrument.service;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Service that uses vendor-provided transformers to convert XML to vendor message objects
 */
@Service
public class XmlTransformerService {
    
    private static final Logger log = LoggerFactory.getLogger(XmlTransformerService.class);
    
    private final MeterRegistry meterRegistry;
    
    // Vendor transformers
    private final com.vendor.instruments.equity.EquityMessageTransformer equityTransformer;
    private final com.vendor.instruments.bond.BondMessageTransformer bondTransformer;
    private final com.vendor.instruments.structuredproduct.StructuredProductMessageTransformer structuredProductTransformer;
    private final com.vendor.instruments.warrant.WarrantMessageTransformer warrantTransformer;
    
    public XmlTransformerService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        // Initialize vendor transformers
        this.equityTransformer = new com.vendor.instruments.equity.EquityMessageTransformer();
        this.bondTransformer = new com.vendor.instruments.bond.BondMessageTransformer();
        this.structuredProductTransformer = new com.vendor.instruments.structuredproduct.StructuredProductMessageTransformer();
        this.warrantTransformer = new com.vendor.instruments.warrant.WarrantMessageTransformer();
        
        log.info("Initialized XML transformer service with vendor transformers");
    }
    
    /**
     * Transform equity XML to vendor EquityMessage object
     */
    public com.vendor.instruments.equity.EquityMessage transformEquityXml(String xmlContent) {
        try {
            log.debug("Transforming equity XML");
            
            com.vendor.instruments.equity.EquityMessage vendorMessage = 
                equityTransformer.transformFromXml(xmlContent);
            
            meterRegistry.counter("instrument.xml.transform.success",
                "type", "equity").increment();
            
            return vendorMessage;
            
        } catch (Exception e) {
            log.error("Error transforming equity XML", e);
            meterRegistry.counter("instrument.xml.transform.errors",
                "type", "equity",
                "error", e.getClass().getSimpleName()).increment();
            throw new XmlTransformationException("Failed to transform equity XML", e);
        }
    }
    
    /**
     * Transform bond XML to vendor BondMessage object
     */
    public com.vendor.instruments.bond.BondMessage transformBondXml(String xmlContent) {
        try {
            log.debug("Transforming bond XML");
            
            com.vendor.instruments.bond.BondMessage vendorMessage = 
                bondTransformer.transformFromXml(xmlContent);
            
            meterRegistry.counter("instrument.xml.transform.success",
                "type", "bond").increment();
            
            return vendorMessage;
            
        } catch (Exception e) {
            log.error("Error transforming bond XML", e);
            meterRegistry.counter("instrument.xml.transform.errors",
                "type", "bond",
                "error", e.getClass().getSimpleName()).increment();
            throw new XmlTransformationException("Failed to transform bond XML", e);
        }
    }
    
    /**
     * Transform structured product XML
     */
    public com.vendor.instruments.structuredproduct.StructuredProductMessage transformStructuredProductXml(
            String xmlContent) {
        try {
            log.debug("Transforming structured product XML");
            
            com.vendor.instruments.structuredproduct.StructuredProductMessage vendorMessage = 
                structuredProductTransformer.transformFromXml(xmlContent);
            
            meterRegistry.counter("instrument.xml.transform.success",
                "type", "structured_product").increment();
            
            return vendorMessage;
            
        } catch (Exception e) {
            log.error("Error transforming structured product XML", e);
            meterRegistry.counter("instrument.xml.transform.errors",
                "type", "structured_product",
                "error", e.getClass().getSimpleName()).increment();
            throw new XmlTransformationException("Failed to transform structured product XML", e);
        }
    }
    
    /**
     * Transform warrant XML
     */
    public com.vendor.instruments.warrant.WarrantMessage transformWarrantXml(String xmlContent) {
        try {
            log.debug("Transforming warrant XML");
            
            com.vendor.instruments.warrant.WarrantMessage vendorMessage = 
                warrantTransformer.transformFromXml(xmlContent);
            
            meterRegistry.counter("instrument.xml.transform.success",
                "type", "warrant").increment();
            
            return vendorMessage;
            
        } catch (Exception e) {
            log.error("Error transforming warrant XML", e);
            meterRegistry.counter("instrument.xml.transform.errors",
                "type", "warrant",
                "error", e.getClass().getSimpleName()).increment();
            throw new XmlTransformationException("Failed to transform warrant XML", e);
        }
    }
    
    /**
     * Custom exception for transformation errors
     */
    public static class XmlTransformationException extends RuntimeException {
        public XmlTransformationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
