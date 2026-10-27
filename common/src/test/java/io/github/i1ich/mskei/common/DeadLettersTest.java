package io.github.i1ich.mskei.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class DeadLettersTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void keepsOriginalBytesAndHeadersAndAddsErrorHeaders() {
        RecordHeaders original = new RecordHeaders();
        original.add(EventHeaders.EVENT_ID, b("e-1"));
        original.add(EventHeaders.ERROR_CLASS, b("OldError"));

        ProducerRecord<byte[], byte[]> r = DeadLetters.record(
                new DeadLetters.Source("sf.change-events.raw", 2, 42L), b("k"), b("v"), original,
                new IllegalArgumentException("bad"), 1, Instant.parse("2026-10-27T10:00:00Z"));

        assertThat(r.topic()).isEqualTo(Topics.DLQ);
        assertThat(r.key()).isEqualTo(b("k"));
        assertThat(r.value()).isEqualTo(b("v"));
        assertThat(DeadLetters.header(r.headers(), EventHeaders.EVENT_ID)).isEqualTo("e-1");
        assertThat(DeadLetters.header(r.headers(), EventHeaders.ERROR_CLASS))
                .isEqualTo(IllegalArgumentException.class.getName());
        assertThat(r.headers().headers(EventHeaders.ERROR_CLASS)).hasSize(1);
        assertThat(DeadLetters.header(r.headers(), EventHeaders.ERROR_MESSAGE)).isEqualTo("bad");
        assertThat(DeadLetters.header(r.headers(), EventHeaders.SOURCE_TOPIC)).isEqualTo("sf.change-events.raw");
        assertThat(DeadLetters.header(r.headers(), EventHeaders.SOURCE_PARTITION)).isEqualTo("2");
        assertThat(DeadLetters.header(r.headers(), EventHeaders.SOURCE_OFFSET)).isEqualTo("42");
        assertThat(DeadLetters.header(r.headers(), EventHeaders.FAILED_AT)).isEqualTo("2026-10-27T10:00:00Z");
        assertThat(DeadLetters.header(r.headers(), EventHeaders.ATTEMPTS)).isEqualTo("1");
    }

    @Test
    void truncatesLongMessagesAndHandlesNulls() {
        ProducerRecord<byte[], byte[]> r = DeadLetters.record(new DeadLetters.Source("t", 0, 0), null, null, null,
                new RuntimeException("x".repeat(5000)), 3, Instant.EPOCH);
        assertThat(DeadLetters.header(r.headers(), EventHeaders.ERROR_MESSAGE)).hasSize(DeadLetters.MAX_MESSAGE_CHARS);
        assertThat(DeadLetters.header(r.headers(), "missing")).isNull();
        assertThat(DeadLetters.header(null, "x")).isNull();
    }

    @Test
    void schemaRegistryConfig() {
        assertThat(SchemaRegistryConfig.from(Map.of("SCHEMA_REGISTRY_URL", "http://sr:8081")))
                .containsEntry("schema.registry.url", "http://sr:8081")
                .containsEntry("auto.register.schemas", true);
        assertThat(SchemaRegistryConfig.from(Map.of("SCHEMA_REGISTRY_URL", "u", "SCHEMA_AUTO_REGISTER", "false")))
                .containsEntry("auto.register.schemas", false)
                .containsEntry("use.latest.version", true);
        assertThatThrownBy(() -> SchemaRegistryConfig.from(Map.of())).isInstanceOf(IllegalStateException.class);
    }
}
