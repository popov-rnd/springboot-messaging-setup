package com.popovrnd.producercloud.web;

import com.popovrnd.producercloud.domain.JobMessage;
import com.popovrnd.producercloud.service.MessageProducer;
import com.popovrnd.producercloud.web.request.JobRequest;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IngestController {

    private static final Logger log = LoggerFactory.getLogger(IngestController.class);

    private final MessageProducer producer;

    public IngestController(@Qualifier("imperative") MessageProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/ingest")
    public String sendMessage(@Valid @RequestBody JobRequest request) {

        JobMessage message = JobMessage.of(
                request.type(),
                request.source(),
                request.payload(),
                request.correlationId()
        );

        producer.sendMessage(message);
        log.debug("Message has been sent! {}, thread = {}", message, Thread.currentThread());
        return "✅ Sent: " + message;
    }

}
