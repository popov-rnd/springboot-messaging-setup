package com.popovrnd.producercloud.service;

import com.popovrnd.producercloud.domain.JobMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service("imperative")
public class MessageProducer {

    private static final Logger log = LoggerFactory.getLogger(MessageProducer.class);

    // Keep binding name externalized so it’s easy to change per env/profile
    private final String bindingName;

    private final StreamBridge streamBridge;

    public MessageProducer(
            StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
        this.bindingName = "popov-rnd-out-0";
    }

    /**
     * Throws on failure so callers can decide to retry/outbox, etc.
     */
    public void sendMessage(JobMessage payload) {
        Message<JobMessage> message = MessageBuilder
                .withPayload(payload)
                .setHeader("event-source", "producer-cloud")
                // Add tracing/correlation if you use Micrometer/observability
                //.setHeader("trace-id", MDC.get("traceId"))
                .build();

        boolean accepted = streamBridge.send(bindingName, message);
        if (!accepted) {
            // Usually false only if no such binding exists; treat as misconfiguration
            throw new IllegalStateException("Binding '" + bindingName + "' rejected the message");
        }
        log.debug("Sent via {} -> {}", bindingName, payload);
    }
}

