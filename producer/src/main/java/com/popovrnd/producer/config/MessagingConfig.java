package com.popovrnd.producer.config;

import com.popovrnd.producer.service.ProducerProps;
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

    private final ProducerProps props;

    public MessagingConfig(ProducerProps props) {
        this.props = props;
    }

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
        return t;
    }
}
