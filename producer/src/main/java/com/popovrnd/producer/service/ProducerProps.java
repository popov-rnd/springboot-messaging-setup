package com.popovrnd.producer.service;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "producer")
public record ProducerProps(
        String exchange,
        String routingKey,
        String queue,
        String dlx,
        String dlq
) {}