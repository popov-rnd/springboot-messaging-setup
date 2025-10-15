package com.popovrnd.producercloud.config;


import com.popovrnd.producercloud.domain.JobMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Supplier;

/**
 * Reactive producer config.
 * Provides:
 *  - A bounded in-memory buffer (Sink) for JobMessage.
 *  - A reactive Supplier<Flux<JobMessage>> that the Rabbit binder drains.
 * Flow:  HTTP → Sink → Supplier → RabbitMQ
 */
@Configuration("reactive")
public class MessageProducer {

    private static final Logger log = LoggerFactory.getLogger(MessageProducer.class);

    /**
     * 🟩 1️⃣  Reactive in-memory buffer between HTTP and Rabbit binder
     * Bounded, single-subscriber buffer between web layer and Rabbit binder.
     * - Prevents blocking.
     * - Drops with FAIL_OVERFLOW when full (10k messages).
     */
    @Bean
    public Sinks.Many<JobMessage> jobSink() {
        return Sinks.many()
                // One consumer (the Rabbit binder) → ensures ordered delivery
                .unicast()
                // Bounded buffer (10 000 messages) to prevent unbounded growth;
                .onBackpressureBuffer(new ArrayBlockingQueue<>(10_000));
    }

    /**
     * 🟩 2️⃣  Functional producer (Spring Cloud Stream will bind this to Rabbit)
     * Exposes the Sink as a reactive Flux for the binder to consume.
     * The binder drains messages asynchronously to RabbitMQ.
     */
   /* @Bean
    Supplier<Flux<JobMessage>> popovRnd(Sinks.Many<JobMessage> sink) {
        return sink::asFlux;
    }*/

    @Bean
    public Supplier<Flux<JobMessage>> popovRnd(Sinks.Many<JobMessage> sink) {
        return () -> {
            log.info("✅ popovRnd Supplier initialized — exposing Flux to binder");
            return sink.asFlux()
                    .doOnSubscribe(sub -> log.info("📨 Binder subscribed to popovRnd Flux"))
                    .doOnNext(msg -> log.debug("🚀 Emitting message to Rabbit: {}", msg))
                    .doOnError(err -> log.error("❌ Error in popovRnd Flux", err))
                    .doOnCancel(() -> log.warn("⚠️ Binder unsubscribed from popovRnd Flux"))
                    .doOnComplete(() -> log.info("🏁 popovRnd Flux completed"));
        };
    }
}

