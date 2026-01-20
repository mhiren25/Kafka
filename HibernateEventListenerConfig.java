package com.example.instrument.config;

import com.example.instrument.domain.HibernateAuditEventListener;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.hibernate.internal.SessionFactoryImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.persistence.EntityManagerFactory;

/**
 * Configuration to register Hibernate event listeners for audit logging
 */
@Configuration
public class HibernateEventListenerConfig {
    
    private final EntityManagerFactory entityManagerFactory;
    private final HibernateAuditEventListener auditEventListener;
    
    @Autowired
    public HibernateEventListenerConfig(
            EntityManagerFactory entityManagerFactory,
            HibernateAuditEventListener auditEventListener) {
        this.entityManagerFactory = entityManagerFactory;
        this.auditEventListener = auditEventListener;
    }
    
    @PostConstruct
    public void registerListeners() {
        SessionFactoryImpl sessionFactory = entityManagerFactory.unwrap(SessionFactoryImpl.class);
        EventListenerRegistry registry = sessionFactory.getServiceRegistry()
            .getService(EventListenerRegistry.class);
        
        // Register pre-update listener for audit logging
        registry.appendListeners(EventType.PRE_UPDATE, auditEventListener);
        
        // Could also register other listeners:
        // registry.appendListeners(EventType.PRE_INSERT, auditEventListener);
        // registry.appendListeners(EventType.PRE_DELETE, auditEventListener);
    }
}
