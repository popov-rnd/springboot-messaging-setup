package com.popovrnd.producer.service;

import com.popovrnd.producer.service.domain.JobMessage;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

@Service
public class MessageProducer {

    private final RabbitTemplate rabbitTemplate;
    private final ProducerProps props;

    public MessageProducer(RabbitTemplate rabbitTemplate, ProducerProps props) {
        this.rabbitTemplate = rabbitTemplate;
        this.props = props;
    }

    public void sendMessage(JobMessage message) {
        rabbitTemplate.convertAndSend(props.exchange(), props.routingKey(), message);
        System.out.println("✅ Sent: " + message);
    }
}
