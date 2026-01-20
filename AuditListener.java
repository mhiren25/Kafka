package com.example.instrument.domain;

import io.micrometer.core.instrument.MeterRegistry;
import org.hibernate.event.spi.*;
import org.hibernate.persister.entity.EntityPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.persistence.PreUpdate;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * JPA Entity Listener for audit logging
 * Logs field changes from old value → new value
 */
public class AuditListener {
    
    private static final Logger auditLog = LoggerFactory.getLogger("AUDIT");
    
    @PreUpdate
    public void preUpdate(Object entity) {
        // This will be called before Hibernate updates the entity
        // We'll use Hibernate interceptor for actual old vs new comparison
        auditLog.debug("Entity update detected: {}", entity.getClass().getSimpleName());
    }
}

/**
 * Hibernate Event Listener for detailed change tracking
 * This captures the actual old and new values
 */
@Component
public class HibernateAuditEventListener implements PreUpdateEventListener {
    
    private static final Logger auditLog = LoggerFactory.getLogger("AUDIT");
    private static final Logger log = LoggerFactory.getLogger(HibernateAuditEventListener.class);
    
    private final MeterRegistry meterRegistry;
    
    @Autowired
    public HibernateAuditEventListener(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }
    
    @Override
    public boolean onPreUpdate(PreUpdateEvent event) {
        Object entity = event.getEntity();
        EntityPersister persister = event.getPersister();
        
        String[] propertyNames = persister.getPropertyNames();
        Object[] oldState = event.getOldState();
        Object[] newState = event.getState();
        
        Map<String, FieldChange> changes = new HashMap<>();
        
        for (int i = 0; i < propertyNames.length; i++) {
            String propertyName = propertyNames[i];
            Object oldValue = oldState != null ? oldState[i] : null;
            Object newValue = newState[i];
            
            // Check if value changed
            if (hasChanged(oldValue, newValue)) {
                changes.put(propertyName, new FieldChange(
                    propertyName,
                    formatValue(oldValue),
                    formatValue(newValue)
                ));
            }
        }
        
        // Log changes if any
        if (!changes.isEmpty()) {
            logAuditChanges(entity, changes);
        }
        
        return false; // false means continue with update
    }
    
    private boolean hasChanged(Object oldValue, Object newValue) {
        if (oldValue == null && newValue == null) {
            return false;
        }
        if (oldValue == null || newValue == null) {
            return true;
        }
        return !oldValue.equals(newValue);
    }
    
    private String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        }
        return value.toString();
    }
    
    private void logAuditChanges(Object entity, Map<String, FieldChange> changes) {
        String entityType = entity.getClass().getSimpleName();
        String instrumentId = extractInstrumentId(entity);
        Long version = extractVersion(entity);
        
        // Structured audit log
        StringBuilder auditMessage = new StringBuilder();
        auditMessage.append(String.format(
            "ENTITY_UPDATE: type=%s, instrumentId=%s, version=%s, changes=[",
            entityType, instrumentId, version
        ));
        
        boolean first = true;
        for (FieldChange change : changes.values()) {
            if (!first) {
                auditMessage.append(", ");
            }
            auditMessage.append(String.format(
                "%s: '%s' → '%s'",
                change.getFieldName(),
                change.getOldValue(),
                change.getNewValue()
            ));
            first = false;
            
            // Emit metric per field change
            meterRegistry.counter("instrument.field.changed",
                "entity", entityType,
                "field", change.getFieldName()
            ).increment();
        }
        
        auditMessage.append("]");
        
        auditLog.info(auditMessage.toString());
        
        // Also emit summary metric
        meterRegistry.counter("instrument.entity.updated",
            "entity", entityType,
            "field_count", String.valueOf(changes.size())
        ).increment();
    }
    
    private String extractInstrumentId(Object entity) {
        try {
            Field field = entity.getClass().getDeclaredField("instrumentId");
            field.setAccessible(true);
            Object value = field.get(entity);
            return value != null ? value.toString() : "UNKNOWN";
        } catch (Exception e) {
            return "UNKNOWN";
        }
    }
    
    private Long extractVersion(Object entity) {
        try {
            Field field = entity.getClass().getDeclaredField("version");
            field.setAccessible(true);
            return (Long) field.get(entity);
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Inner class to hold field change information
     */
    private static class FieldChange {
        private final String fieldName;
        private final String oldValue;
        private final String newValue;
        
        public FieldChange(String fieldName, String oldValue, String newValue) {
            this.fieldName = fieldName;
            this.oldValue = oldValue;
            this.newValue = newValue;
        }
        
        public String getFieldName() {
            return fieldName;
        }
        
        public String getOldValue() {
            return oldValue;
        }
        
        public String getNewValue() {
            return newValue;
        }
    }
}
