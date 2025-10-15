package com.popovrnd.consumer.service;

import com.popovrnd.consumer.service.domain.JobMessage;
import com.popovrnd.consumer.service.exceptions.NonRetryableBusinessException;
import com.popovrnd.consumer.service.utils.AckUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

import java.io.IOException;

@Component
public class MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    private final MessageProcessor messageProcessor;

    private final AckUtils ackUtils;

    public MessageConsumer(MessageProcessor messageProcessor, AckUtils ackUtils) {
        this.messageProcessor = messageProcessor;
        this.ackUtils=ackUtils;
    }

    /**
     * Listens for messages from RabbitMQ.
     * The binding below automatically ensures queue, exchange, and routing key exist.
     */
    @RabbitListener(queues = "${consumer.queue}")
    public void onMessage(JobMessage jobMessage, Message message, Channel channel) throws IOException {

        log.info("Received message with thread = {}", Thread.currentThread());

        log.info("Received message from [{}]: {}", message.getMessageProperties().getConsumerQueue(), jobMessage);

        long tag = message.getMessageProperties().getDeliveryTag();

        log.info("Next tag is = {}", tag);

        try {
            // Simulate message processing logic
            messageProcessor.process(jobMessage);              // Your business logic
            ackUtils.ok(channel, message);                     // Ack on real success
        } catch (NonRetryableBusinessException e) {
            log.error("Not retryable business exception: {}", jobMessage, e);
            ackUtils.dead(channel, message);                   // Send to DLQ via DLX
        } catch (Exception e) {
            log.error("Retryable business exception: {}", jobMessage, e);
            ackUtils.retry(channel, message);                  // → Broker redelivers; quorum counts
        }
    }
}

