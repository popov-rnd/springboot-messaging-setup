package com.popovrnd.consumercloud.service;

import com.popovrnd.consumercloud.service.domain.JobMessage;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
public class MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    private final MessageProcessor messageProcessor;

    public MessageConsumer(MessageProcessor messageProcessor) {
        this.messageProcessor = messageProcessor;
    }

    @Bean//(name = "popovRnd")
    public Consumer<Message<JobMessage>> popovRnd() {
        return msg -> {
            JobMessage jm = msg.getPayload();
            Channel channel = msg.getHeaders().get(AmqpHeaders.CHANNEL, Channel.class);
            Long deliveryTag = msg.getHeaders().get(AmqpHeaders.DELIVERY_TAG, Long.class);

            try {
                // --- Your business logic ---
                log.debug("Processing JobMessage id={} thread ={}", jm.id(), Thread.currentThread());
                messageProcessor.process(jm);

                // Success -> MANUAL ACK
                if (channel != null && deliveryTag != null) {
                    channel.basicAck(deliveryTag, false);
                }
            } catch (Exception ex) {
                log.warn("Job failed: id={}, reason={}", jm.id(), ex.toString());
                // IMPORTANT: don't ack here; let binder retry and finally route to DLQ
                // Triggers retry; after maxAttempts, binder republishToDlq -> DLQ
                throw new RuntimeException(ex);
            }
        };
    }
}
