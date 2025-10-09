package com.popovrnd.consumer.service.utils;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class AckUtils {

    public void ok(Channel ch, Message m) throws IOException {
        ch.basicAck(tag(m), false);
    }

    public void retry(Channel ch, Message m) throws IOException {
        ch.basicNack(tag(m), false, true);
    }

    public void dead(Channel ch, Message m) throws IOException {
        ch.basicReject(tag(m), false);
    }

    private long tag(Message m) {
        return m.getMessageProperties().getDeliveryTag();
    }
}
