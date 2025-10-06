package com.popovrnd.producer.web.request;

import jakarta.validation.constraints.*;

/**
 * Represents a job submitted via HTTP API — closely aligned with SampleMessage.
 */
public record JobRequest(
        @NotBlank String type,        // Logical job type, e.g. "DATA_EXPORT"
        @NotBlank String source,      // Origin service/module (optional for external clients)
        @NotNull Object payload,      // Core job data — can be any structured object
        String correlationId          // Optional: to link related requests
) {}
