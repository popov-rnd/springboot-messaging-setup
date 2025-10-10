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

/**
 * Configures a RabbitMQ quorum queue (modern).
 * <p>
 * Quorum queues are the modern, replicated queue type in RabbitMQ.
 * They are based on the Raft consensus algorithm and are recommended
 * for all production workloads instead of classic queues.
 * <p>
 * Key characteristics:
 * <ul>
 *   <li><b>Durable and replicated</b> — message data is persisted and
 *       replicated across cluster nodes using Raft.</li>
 *   <li><b>Delivery-limit support</b> — the broker can automatically
 *       count message delivery attempts and dead-letter messages
 *       after the limit is reached (no app-side retry counters needed).</li>
 *   <li><b>Crash-safe</b> — can recover cleanly even if a node fails
 *       during message processing.</li>
 * </ul>
 * <p>
 * When running on a <b>single-node</b> RabbitMQ instance (e.g. local Docker),
 * the quorum queue is still fully functional: the single node acts as both
 * the leader and the only replica. Raft replication logic remains active,
 * but since there are no followers, the queue simply logs locally with
 * minimal overhead (~5–10% slower than a classic queue).
 * <p>
 * No configuration changes are required when scaling to multiple nodes —
 * additional nodes will automatically join the Raft quorum and replicate
 * this queue for high availability.
 *
 * @return a durable quorum queue named "popov-rnd.queue"
 */
@Configuration
public class MessagingConfig {

    private static final Logger log = LoggerFactory.getLogger(MessagingConfig.class);

    private final ProducerProps props;

    public MessagingConfig(ProducerProps props) {
        this.props = props;
    }

    @Bean
    public Queue mainQueue() {
        return QueueBuilder
                // 🧱 Durable queue survives broker restarts
                //    In quorum mode, durability is mandatory anyway.
                .durable(props.queue())

                // 🧩 Define this queue as a Quorum queue
                //    → Replicated across quorum nodes via Raft.
                //    → Always persisted to disk, fault-tolerant.
                .withArgument("x-queue-type", "quorum")

                // ♻️ Delivery-limit for re-delivery attempts
                //    → After 3 failed deliveries, message moves to DLQ.
                //    → Prevents infinite redelivery loops.
                .withArgument("x-delivery-limit", 3)

                // 📦 Maximum number of messages stored in the queue.
                //    → Prevents unbounded growth.
                //    → Combined with 'x-overflow' below.
                //    ⚠️ Tune via broker policy in prod if possible.
                .withArgument("x-max-length", 10_000)

                // 🚦 Overflow strategy when queue is full:
                //    "reject-publish" = reject new messages instead of
                //    silently dropping old ones (no data loss).
                //    Producer receives basic.return → handle via ReturnsCallback.
                .withArgument("x-overflow", "reject-publish")

                // 🧠 Memory safeguard (since RabbitMQ 3.12):
                //    → Limit how much stays in RAM; excess is paged to disk.
                //    → Helps prevent broker OOM during bursts.
                .withArgument("x-max-in-memory-length-bytes", 50_000_000)

                // ⚰️ Dead-letter configuration:
                //    → When message exceeds delivery-limit or is rejected,
                //      it’s routed to this DLX for later inspection.
                .withArgument("x-dead-letter-exchange", props.dlx())

                // 🔁 DLQ routing key (matches binding on DLQ side)
                .withArgument("x-dead-letter-routing-key", props.routingKey() + ".dlq")

                // ✅ Build final Queue instance
                .build();
    }


    @Bean
    public Queue dlqQueue() {
        return QueueBuilder
                .durable(props.dlq())
                .build();
    }

    @Bean
    public DirectExchange mainExchange() {
        return ExchangeBuilder
                .directExchange(props.exchange())
                .durable(true)
                .build();
    }

    @Bean
    public DirectExchange dlxExchange() {
        return ExchangeBuilder
                .directExchange(props.dlx())
                .durable(true)
                .build();
    }

    @Bean
    public Binding bindMain() {
        return BindingBuilder
                .bind(mainQueue())
                .to(mainExchange())
                .with(props.routingKey());
    }

    @Bean
    public Binding bindDlq() {
        return BindingBuilder
                .bind(dlqQueue())
                .to(dlxExchange())
                .with(props.routingKey() + ".dlq");
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
