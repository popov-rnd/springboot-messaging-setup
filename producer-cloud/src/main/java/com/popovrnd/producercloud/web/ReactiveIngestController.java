package com.popovrnd.producercloud.web;

import com.popovrnd.producercloud.domain.JobMessage;
import com.popovrnd.producercloud.web.request.JobRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import jakarta.validation.Valid;

@RestController
public class ReactiveIngestController {

    private static final Logger log = LoggerFactory.getLogger(ReactiveIngestController.class);

    // 🟩 Inject the shared Sink bean (defined in MessageProducerConfig)
    private final Sinks.Many<JobMessage> sink;

    public ReactiveIngestController(Sinks.Many<JobMessage> sink) {
        this.sink = sink;
    }

    @PostMapping("/ingest-reactive")
    public Mono<ResponseEntity<String>> ingest(@Valid @RequestBody JobRequest request) {
        return Mono.fromCallable(() -> {
            JobMessage message = JobMessage.of(
                    request.type(),
                    request.source(),
                    request.payload(),
                    request.correlationId()
            );

            var result = sink.tryEmitNext(message);

            log.debug("Sent, success= {}, thread = {}", result.isSuccess(), Thread.currentThread());

            return result.isSuccess()
                    ? ResponseEntity.accepted().body("✅ Queued: " + message)
                    : ResponseEntity.status(503).body("⚠️ Buffer full, try later");
        });
    }
}

