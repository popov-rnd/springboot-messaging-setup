package com.popovrnd.producer.service;

import com.popovrnd.producer.service.domain.JobMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

@Service
public class MessageProducer {

    private static final Logger log = LoggerFactory.getLogger(MessageProducer.class);

    private final RabbitTemplate rabbitTemplate;
    private final ProducerProps props;

    public MessageProducer(RabbitTemplate rabbitTemplate, ProducerProps props) {
        this.rabbitTemplate = rabbitTemplate;
        this.props = props;
    }

    /**
     * Sends messages to a RabbitMQ instance
     *
     * @param jobMessage
     */
    public void sendMessage(JobMessage jobMessage) {
        try {
            CorrelationData correlation = new CorrelationData(jobMessage.id());
            rabbitTemplate.convertAndSend(
                    props.exchange(),
                    props.routingKey(),
                    jobMessage,
                    message -> {
                        message.
                                getMessageProperties().
                                setDeliveryMode(MessageDeliveryMode.PERSISTENT); // Individual messages are stored on disk (not lost if broker restarts)
                        return message;
                    },
                    correlation);
            log.debug("Message is sent = {}", jobMessage);
        } catch (AmqpException ex) {
            log.error("❌ Failed to send after retries, storing for replay. id={}", jobMessage.id(), ex);
            // ⚠️ Save to a DB table or local file for further investigation.
        }
    }
}
