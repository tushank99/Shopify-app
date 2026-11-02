package com.shopify.ledger.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    /**
     * The consumer reads raw bytes, so every record the retry/DLT machinery re-publishes (to
     * order.placed-retry-N and order.placed-dlt) is byte[] too. Writing the original bytes back unchanged means
     * a poison pill lands in the DLT exactly as it was received, and a later redrive can replay it as-is.
     */
    @Bean
    public ProducerFactory<String, byte[]> dltProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // Bounded: a broker outage must not block a consumer thread for the 2-minute default.
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new ByteArraySerializer());
    }

    /** Referenced by name from @RetryableTopic(kafkaTemplate = "dltKafkaTemplate"). */
    @Bean(name = "dltKafkaTemplate")
    public KafkaTemplate<String, byte[]> dltKafkaTemplate(ProducerFactory<String, byte[]> dltProducerFactory) {
        return new KafkaTemplate<>(dltProducerFactory);
    }
}