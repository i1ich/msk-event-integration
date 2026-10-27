package io.github.i1ich.mskei.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class KafkaClientConfigTest {

    @Test
    void plaintextIsDefault() {
        Properties p = KafkaClientConfig.from(Map.of("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        assertThat(p).containsEntry("bootstrap.servers", "localhost:9092")
                .containsEntry("security.protocol", "PLAINTEXT")
                .doesNotContainKey("sasl.mechanism");
    }

    @Test
    void iamSetsSaslSsl() {
        Properties p = KafkaClientConfig.from(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "b-1:9098", "KAFKA_SECURITY", "IAM"));
        assertThat(p).containsEntry("security.protocol", "SASL_SSL")
                .containsEntry("sasl.mechanism", "AWS_MSK_IAM");
    }

    @Test
    void missingBootstrapFails() {
        assertThatThrownBy(() -> KafkaClientConfig.from(Map.of()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void unknownSecurityFails() {
        assertThatThrownBy(() -> KafkaClientConfig.from(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "x:1", "KAFKA_SECURITY", "ssl")))
                .isInstanceOf(IllegalStateException.class);
    }
}
