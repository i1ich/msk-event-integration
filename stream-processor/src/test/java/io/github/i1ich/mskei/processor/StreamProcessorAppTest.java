package io.github.i1ich.mskei.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class StreamProcessorAppTest {

    @Test
    void defaultsAreExactlyOnceWithDlqHandler() {
        Properties p = StreamProcessorApp.streamsProperties(Map.of("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"));

        assertThat(p.get("application.id")).isEqualTo("stream-processor");
        assertThat(p.get("processing.guarantee")).isEqualTo("exactly_once_v2");
        assertThat(p.get("bootstrap.servers")).isEqualTo("kafka:9092");
        assertThat(p.get("default.deserialization.exception.handler"))
                .isEqualTo(DlqDeserializationExceptionHandler.class);
        assertThat(p).doesNotContainKey("replication.factor");
    }

    @Test
    void envOverridesApplicationIdAndReplicationFactor() {
        Properties p = StreamProcessorApp.streamsProperties(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092",
                "STREAMS_APPLICATION_ID", "sp-blue",
                "STREAMS_REPLICATION_FACTOR", "3"));

        assertThat(p.get("application.id")).isEqualTo("sp-blue");
        assertThat(p.get("replication.factor")).isEqualTo("3");
    }

    @Test
    void missingBootstrapServersFailsFast() {
        assertThatThrownBy(() -> StreamProcessorApp.streamsProperties(Map.of()))
                .isInstanceOf(IllegalStateException.class);
    }
}
