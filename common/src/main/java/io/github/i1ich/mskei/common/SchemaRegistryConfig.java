package io.github.i1ich.mskei.common;

import java.util.HashMap;
import java.util.Map;

/**
 * Serde settings for Confluent Schema Registry (local). AWS Glue Schema Registry is added in M4 behind
 * the same method, selected by config.
 *
 * <ul>
 *   <li>{@code SCHEMA_REGISTRY_URL} — required, e.g. {@code http://localhost:8081}.</li>
 *   <li>{@code SCHEMA_AUTO_REGISTER} — {@code true} by default locally; set {@code false} where schemas are
 *       registered only by CI / {@code scripts/register-schemas.sh}.</li>
 * </ul>
 */
public final class SchemaRegistryConfig {
    public static final String ENV_URL = "SCHEMA_REGISTRY_URL";
    public static final String ENV_AUTO_REGISTER = "SCHEMA_AUTO_REGISTER";

    private SchemaRegistryConfig() {
    }

    public static Map<String, Object> fromEnv() {
        return from(System.getenv());
    }

    public static Map<String, Object> from(Map<String, String> env) {
        String url = env.get(ENV_URL);
        if (url == null || url.isBlank()) {
            throw new IllegalStateException(ENV_URL + " is not set");
        }
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("schema.registry.url", url);
        boolean autoRegister = Boolean.parseBoolean(env.getOrDefault(ENV_AUTO_REGISTER, "true"));
        cfg.put("auto.register.schemas", autoRegister);
        if (!autoRegister) {
            // Schemas are registered from schemas/*.avsc by CI / register-schemas.sh. The Avro-generated classes
            // carry extra properties (avro.java.string), so their schema text never matches the registered one
            // exactly and a plain lookup fails with 40403. Use the latest registered version instead; the serializer
            // still checks that it is compatible with the record's own schema (latest.compatibility.strict).
            cfg.put("use.latest.version", true);
        }
        return cfg;
    }
}
