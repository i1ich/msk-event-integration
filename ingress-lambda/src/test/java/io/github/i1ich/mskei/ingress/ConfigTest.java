package io.github.i1ich.mskei.ingress;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.i1ich.mskei.common.KafkaClientConfig;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class ConfigTest {
    @Test
    void ingressConfigDefaults() {
        IngressConfig c = IngressConfig.from(Map.of());
        assertThat(c.maxBodyBytes()).isEqualTo(262_144);
        assertThat(c.hmacEnabled()).isFalse();
    }

    @Test
    void ingressConfigFromEnv() {
        IngressConfig c = IngressConfig.from(Map.of("INGRESS_MAX_BODY_BYTES", " 500 ", "INGRESS_HMAC_SECRET", "x"));
        assertThat(c.maxBodyBytes()).isEqualTo(500);
        assertThat(c.hmacEnabled()).isTrue();
        assertThat(IngressConfig.from(Map.of("INGRESS_HMAC_SECRET", " ")).hmacEnabled()).isFalse();
        assertThat(IngressConfig.from(Map.of("INGRESS_MAX_BODY_BYTES", "")).maxBodyBytes()).isEqualTo(262_144);
    }

    @Test
    void producerIsIdempotentAndBounded() {
        Properties conn = KafkaClientConfig.from(Map.of("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        Properties p = ProducerFactory.producerProperties(conn);
        assertThat(p).containsEntry("bootstrap.servers", "localhost:9092")
                .containsEntry("enable.idempotence", "true")
                .containsEntry("acks", "all")
                .containsEntry("linger.ms", "0")
                .containsEntry("delivery.timeout.ms", "10000")
                .containsEntry("request.timeout.ms", "5000")
                .containsEntry("max.block.ms", "5000");
        assertThat(p.getProperty("key.serializer")).endsWith("StringSerializer");
    }
}
