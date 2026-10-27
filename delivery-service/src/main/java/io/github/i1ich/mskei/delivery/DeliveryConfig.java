package io.github.i1ich.mskei.delivery;

import java.time.Duration;
import java.util.Map;

/**
 * Settings from environment variables. Durations are ISO-8601 (PT5S, PT1M).
 *
 * <p>RETRY_DELAY_1M / RETRY_DELAY_10M only change the {@code not-before} time; the topics are still called
 * retry.1m and retry.10m. Local docker compose sets short delays (e.g. PT5S / PT15S) so the demo does not
 * take 11 minutes.
 */
public record DeliveryConfig(String downstreamUrl, Duration retryDelay1m, Duration retryDelay10m,
        Duration connectTimeout, Duration requestTimeout) {

    public static DeliveryConfig fromEnv() {
        return from(System.getenv());
    }

    public static DeliveryConfig from(Map<String, String> env) {
        String url = env.get("DOWNSTREAM_URL");
        if (url == null || url.isBlank()) {
            throw new IllegalStateException("DOWNSTREAM_URL is not set");
        }
        return new DeliveryConfig(url,
                duration(env, "RETRY_DELAY_1M", "PT1M"),
                duration(env, "RETRY_DELAY_10M", "PT10M"),
                duration(env, "HTTP_CONNECT_TIMEOUT", "PT2S"),
                duration(env, "HTTP_TIMEOUT", "PT5S"));
    }

    private static Duration duration(Map<String, String> env, String name, String defaultValue) {
        return Duration.parse(env.getOrDefault(name, defaultValue));
    }
}
