package com.popovrnd.producercloud.domain;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

/**
 * A lean, production-style message DTO for RabbitMQ or any message broker.
 */
public record JobMessage(
        String id,          // Unique message ID for traceability
        String type,        // Message type, e.g. "USER_CREATED", "JOB_STARTED"
        String source,      // Origin service or module
        Object payload,     // Actual data (can be DTO, Map, or simple text)
        Date createdAt,  // Message creation timestamp
        String correlationId // Optional, to link related messages
) implements Serializable {

    public static JobMessage of(String type, String source, Object payload, String correlationId) {
        return new JobMessage(
                UUID.randomUUID().toString(),
                type,
                source,
                payload,
                new Date(),
                correlationId
        );
    }
}

