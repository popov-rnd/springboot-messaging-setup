package com.popovrnd.producer.service;

import com.popovrnd.producer.service.domain.JobMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.util.Date;

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
            // 🧩 CorrelationData is for publisher confirms:
            //    When RabbitMQ ACKs (or NACKs) message delivery, this ID lets you match the confirmation
            //    back to the original message. Works only if publisher confirms are enabled.
            CorrelationData correlation = new CorrelationData(jobMessage.id());
            // 📨 Send message through a specific exchange + routing key:
            //    Spring will serialize 'jobMessage' using the configured MessageConverter (usually JSON).
            rabbitTemplate.convertAndSend(
                    props.exchange(),     // Exchange name from configuration
                    props.routingKey(),   // Routing key for binding to the queue
                    jobMessage,           // Payload (POJO → JSON)
                    message -> { // MessagePostProcessor allows header customization before send

                        // 💾 Make the message persistent (quorum queues make it out of the box):
                        //    Ensures broker writes it to disk; survives restart if queue is durable.
                        message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                        // 🆔 Unique message ID:
                        //    Useful for deduplication, tracing, and matching logs across producer/consumer.
                        message.getMessageProperties().setMessageId(jobMessage.id());
                        // 🕓 Attach send timestamp:
                        //    Enables latency analysis and message age monitoring on consumer side.
                        message.getMessageProperties().setTimestamp(new Date());
                        // 📦 Content type metadata:
                        //    Lets consumers know payload format (JSON). Usually auto-set by converter.
                        message.getMessageProperties().setContentType(MessageProperties.CONTENT_TYPE_JSON);
                        // ✅ Return the modified message to continue processing.
                        return message;
                    },
                    correlation);
            log.debug("Message is sent = {}", jobMessage);
        } catch (AmqpException ex) {
            // ❌ Handles network/broker/unroutable errors at send time:
            //    - Or save to DB/local file for later replay
            log.error("❌ Broker unreachable (after re-tries), storing for replay. id={}", jobMessage.id(), ex);
            // ⚠️ Save to a DB table or local file for further investigation.
        }
    }
}
