package io.github.i1ich.mskei.ingress;

import io.github.i1ich.mskei.common.KafkaClientConfig;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

/** Builds the Kafka producer. Kept apart so the config can be inspected in tests. */
public final class ProducerFactory {
    private ProducerFactory() {
    }

    /** Connection settings from env (bootstrap, security) plus the settings that make publishing safe. */
    public static Properties producerProperties(Properties connection) {
        Properties props = new Properties();
        props.putAll(connection);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Idempotent producer: the broker drops duplicates caused by internal retries.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Keep everything bounded: API Gateway gives up after 29 s, so we must answer well before that.
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        // One event per request, so batching would only add latency.
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        return props;
    }

    public static Producer<String, String> createFromEnv() {
        return new KafkaProducer<>(producerProperties(KafkaClientConfig.fromEnv()));
    }
}
