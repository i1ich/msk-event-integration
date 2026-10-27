package io.github.i1ich.mskei.common;

import java.util.Map;
import java.util.Properties;

/**
 * Builds the connection part of any Kafka client config from environment variables.
 *
 * <ul>
 *   <li>{@code KAFKA_BOOTSTRAP_SERVERS} — required, e.g. {@code localhost:9092}.</li>
 *   <li>{@code KAFKA_SECURITY} — {@code plaintext} (default, local docker) or {@code iam} (Amazon MSK, TLS + IAM SASL).</li>
 * </ul>
 */
public final class KafkaClientConfig {
    public static final String ENV_BOOTSTRAP = "KAFKA_BOOTSTRAP_SERVERS";
    public static final String ENV_SECURITY = "KAFKA_SECURITY";

    private KafkaClientConfig() {
    }

    public static Properties fromEnv() {
        return from(System.getenv());
    }

    /** Testable variant: takes the environment as a map. */
    public static Properties from(Map<String, String> env) {
        String bootstrap = env.get(ENV_BOOTSTRAP);
        if (bootstrap == null || bootstrap.isBlank()) {
            throw new IllegalStateException(ENV_BOOTSTRAP + " is not set");
        }
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);

        String security = env.getOrDefault(ENV_SECURITY, "plaintext").toLowerCase();
        switch (security) {
            case "plaintext" -> props.put("security.protocol", "PLAINTEXT");
            case "iam" -> {
                // aws-msk-iam-auth picks up credentials from the default AWS provider chain (Lambda/ECS role).
                props.put("security.protocol", "SASL_SSL");
                props.put("sasl.mechanism", "AWS_MSK_IAM");
                props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
                props.put("sasl.client.callback.handler.class",
                        "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
            }
            default -> throw new IllegalStateException(
                    ENV_SECURITY + " must be 'plaintext' or 'iam', got '" + security + "'");
        }
        return props;
    }
}
