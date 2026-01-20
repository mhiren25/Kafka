package com.example.instrument.dto;

import com.example.instrument.domain.InstrumentType;

/**
 * Raw message received from Kafka before XML transformation
 */
public class RawMessage {
    private String xmlContent;
    private String topic;
    private long offset;
    private int partition;
    private String key;
    
    public RawMessage() {
    }
    
    public RawMessage(String xmlContent, String topic, long offset, int partition, String key) {
        this.xmlContent = xmlContent;
        this.topic = topic;
        this.offset = offset;
        this.partition = partition;
        this.key = key;
    }
    
    public String getXmlContent() {
        return xmlContent;
    }
    
    public void setXmlContent(String xmlContent) {
        this.xmlContent = xmlContent;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public void setTopic(String topic) {
        this.topic = topic;
    }
    
    public long getOffset() {
        return offset;
    }
    
    public void setOffset(long offset) {
        this.offset = offset;
    }
    
    public int getPartition() {
        return partition;
    }
    
    public void setPartition(int partition) {
        this.partition = partition;
    }
    
    public String getKey() {
        return key;
    }
    
    public void setKey(String key) {
        this.key = key;
    }
}

/**
 * Message category enum
 */
enum MessageCategory {
    ISSUE,
    TRADING_LINE
}

/**
 * Transformed message after XML parsing and type detection
 * Contains vendor message object and metadata
 */
public class TransformedMessage {
    private Object vendorMessage; // EquityMessage, BondMessage, etc.
    private InstrumentType instrumentType;
    private MessageCategory category;
    private String instrumentId;
    private Long version;
    private String correlationId;
    private String topic;
    private long offset;
    
    public TransformedMessage() {
    }
    
    public Object getVendorMessage() {
        return vendorMessage;
    }
    
    public void setVendorMessage(Object vendorMessage) {
        this.vendorMessage = vendorMessage;
    }
    
    public InstrumentType getInstrumentType() {
        return instrumentType;
    }
    
    public void setInstrumentType(InstrumentType instrumentType) {
        this.instrumentType = instrumentType;
    }
    
    public MessageCategory getCategory() {
        return category;
    }
    
    public void setCategory(MessageCategory category) {
        this.category = category;
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
    
    public String getCorrelationId() {
        return correlationId;
    }
    
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public void setTopic(String topic) {
        this.topic = topic;
    }
    
    public long getOffset() {
        return offset;
    }
    
    public void setOffset(long offset) {
        this.offset = offset;
    }
}
