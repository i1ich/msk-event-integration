package io.github.i1ich.mskei.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

class NotBeforeTest {
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private static RecordHeaders with(String value) {
        RecordHeaders h = new RecordHeaders();
        h.add("not-before", value.getBytes(StandardCharsets.UTF_8));
        return h;
    }

    @Test
    void futureIsNotDue() {
        assertThat(NotBefore.isDue(with("2026-01-01T00:00:01Z"), NOW)).isFalse();
    }

    @Test
    void exactlyNowAndPastAreDue() {
        assertThat(NotBefore.isDue(with("2026-01-01T00:00:00Z"), NOW)).isTrue();
        assertThat(NotBefore.isDue(with("2025-12-31T23:59:59Z"), NOW)).isTrue();
    }

    @Test
    void missingOrBrokenHeaderIsDue() {
        assertThat(NotBefore.isDue(new RecordHeaders(), NOW)).isTrue();
        assertThat(NotBefore.isDue(with("not a date"), NOW)).isTrue();
        assertThat(NotBefore.parse(with("not a date"))).isNull();
    }
}
