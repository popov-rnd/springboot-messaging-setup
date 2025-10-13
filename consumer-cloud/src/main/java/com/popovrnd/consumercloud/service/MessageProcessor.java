package com.popovrnd.consumercloud.service;

import com.popovrnd.consumercloud.service.domain.JobMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    public void process(JobMessage message) {
        try {
            log.debug("Processing message: {}", message);
            // Add your business logic here
            Thread.sleep(10000);
            log.debug("Processing completed: {}", message);
        } catch (Exception e) {
            log.error("Error processing message", e);
        }
    }

}
