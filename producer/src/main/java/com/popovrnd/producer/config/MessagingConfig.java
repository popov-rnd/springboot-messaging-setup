package com.popovrnd.producer.config;

import com.popovrnd.producer.service.ProducerProps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.autoconfigure.amqp.RabbitTemplateConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.amqp.core.*;

@Configuration
public class MessagingConfig {

    private static final Logger log = LoggerFactory.getLogger(MessagingConfig.class);

    private final ProducerProps props;

    public MessagingConfig(ProducerProps props) {
        this.props = props;
    }

    /**
     * The second argument (true) in new Queue(name, durable) makes the queue durable,
     * meaning it will survive broker restarts — it’s stored on disk, not just in memory.
     * @return
     */
    @Bean
    public Queue queue() {
        return new Queue(props.queue(), true);
    }

    @Bean
    public TopicExchange exchange() {
        return new TopicExchange(props.exchange());
    }

    @Bean
    public Binding binding(Queue queue, TopicExchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with(props.routingKey());
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(RabbitTemplateConfigurer configurer,
                                         ConnectionFactory cf,
                                         MessageConverter converter) {
        RabbitTemplate t = new RabbitTemplate();
        configurer.configure(t, cf);             // Applies YAML retry/confirm settings
        t.setMessageConverter(converter);        // Added your JSON converter

        // Triggered when RabbitMQ responds ACK (success) or NACK (failure) for a published message.
        t.setConfirmCallback((correlationData, ack, cause) -> {
            String id = correlationData != null ? correlationData.getId() : "N/A";
            if (ack) {
                // Stored successfully.
                log.debug("✅ Broker confirmed message id={}", id);
            } else {
                // Broker crash, disk full, channel closed, etc.
                log.error("❌ Broker NACKed message id={} cause={}", id, cause);

                // Typical reactions:
                // 1️⃣ Retry sending (if not covered by template.retry)
                // 2️⃣ Store message in "outbox" or "failed" DB table
                // 3️⃣ Send alert / metric to monitoring
            }
        });

        // Triggered when message couldn’t be routed to any queue
        t.setReturnsCallback(returned -> {
            log.error("⚠️ Returned message: exchange={}, routingKey={}, replyCode={}, replyText={}, body={}",
                    returned.getExchange(),
                    returned.getRoutingKey(),
                    returned.getReplyCode(),
                    returned.getReplyText(),
                    new String(returned.getMessage().getBody())
            );

            // Typical reactions:
            // 1️⃣ Investigate missing binding or typo in routing key
            // 2️⃣ Persist to "dead-letter" table for later inspection
            // 3️⃣ Send alert/metric
        });
        return t;
    }
}
