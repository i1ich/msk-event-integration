package io.github.i1ich.mskei.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DeliveryConfigTest {
    @Test
    void defaults() {
        DeliveryConfig c = DeliveryConfig.from(Map.of("DOWNSTREAM_URL", "http://x/downstream"));
        assertThat(c.retryDelay1m()).isEqualTo(Duration.ofMinutes(1));
        assertThat(c.retryDelay10m()).isEqualTo(Duration.ofMinutes(10));
        assertThat(c.connectTimeout()).isEqualTo(Duration.ofSeconds(2));
        assertThat(c.requestTimeout()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void overrides() {
        DeliveryConfig c = DeliveryConfig.from(Map.of("DOWNSTREAM_URL", "http://x", "RETRY_DELAY_1M", "PT5S",
                "RETRY_DELAY_10M", "PT15S", "HTTP_TIMEOUT", "PT1S"));
        assertThat(c.retryDelay1m()).isEqualTo(Duration.ofSeconds(5));
        assertThat(c.retryDelay10m()).isEqualTo(Duration.ofSeconds(15));
        assertThat(c.requestTimeout()).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void downstreamUrlIsRequired() {
        assertThatThrownBy(() -> DeliveryConfig.from(Map.of())).isInstanceOf(IllegalStateException.class);
    }
}
