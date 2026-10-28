package io.github.i1ich.mskei.common;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.services.glue.model.Compatibility;
import software.amazon.awssdk.services.glue.model.DataFormat;

/**
 * Serde settings for the schema registry, chosen per deployment by {@code SCHEMA_REGISTRY_TYPE}.
 *
 * <ul>
 *   <li>{@code SCHEMA_REGISTRY_TYPE} - {@code confluent} (default, local) or {@code glue} (AWS).
 *       The two wire formats are NOT compatible: a record written with one cannot be read with the other.</li>
 *   <li>{@code SCHEMA_AUTO_REGISTER} - {@code true} by default locally; {@code false} where schemas are
 *       registered only by CI / scripts.</li>
 *   <li>confluent: {@code SCHEMA_REGISTRY_URL} (required), e.g. {@code http://localhost:8081}.</li>
 *   <li>glue: {@code GLUE_REGISTRY_NAME} and {@code AWS_REGION} (both required). Credentials come from the
 *       default AWS provider chain (ECS task role on AWS).</li>
 * </ul>
 */
public final class SchemaRegistryConfig {
    public static final String ENV_TYPE = "SCHEMA_REGISTRY_TYPE";
    public static final String ENV_URL = "SCHEMA_REGISTRY_URL";
    public static final String ENV_AUTO_REGISTER = "SCHEMA_AUTO_REGISTER";
    public static final String ENV_GLUE_REGISTRY_NAME = "GLUE_REGISTRY_NAME";
    public static final String ENV_AWS_REGION = "AWS_REGION";

    private SchemaRegistryConfig() {
    }

    public static Map<String, Object> fromEnv() {
        return from(System.getenv());
    }

    public static Map<String, Object> from(Map<String, String> env) {
        String type = env.get(ENV_TYPE);
        boolean autoRegister = Boolean.parseBoolean(env.getOrDefault(ENV_AUTO_REGISTER, "true"));
        if (type == null || type.isBlank() || type.trim().equalsIgnoreCase("confluent")) {
            return confluent(env, autoRegister);
        }
        if (type.trim().equalsIgnoreCase("glue")) {
            return glue(env, autoRegister);
        }
        throw new IllegalStateException(ENV_TYPE + " must be confluent or glue, was " + type);
    }

    /** True if the config map was built for Glue (the Glue registry name key is only present there). */
    public static boolean isGlue(Map<String, ?> cfg) {
        return cfg.containsKey(AWSSchemaRegistryConstants.REGISTRY_NAME);
    }

    private static Map<String, Object> confluent(Map<String, String> env, boolean autoRegister) {
        String url = env.get(ENV_URL);
        if (url == null || url.isBlank()) {
            throw new IllegalStateException(ENV_URL + " is not set");
        }
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("schema.registry.url", url);
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

    private static Map<String, Object> glue(Map<String, String> env, boolean autoRegister) {
        String registry = env.get(ENV_GLUE_REGISTRY_NAME);
        if (registry == null || registry.isBlank()) {
            throw new IllegalStateException(ENV_GLUE_REGISTRY_NAME + " is not set (needed for SCHEMA_REGISTRY_TYPE=glue)");
        }
        String region = env.get(ENV_AWS_REGION);
        if (region == null || region.isBlank()) {
            throw new IllegalStateException(ENV_AWS_REGION + " is not set (needed for SCHEMA_REGISTRY_TYPE=glue)");
        }
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(AWSSchemaRegistryConstants.AWS_REGION, region);
        cfg.put(AWSSchemaRegistryConstants.REGISTRY_NAME, registry);
        cfg.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
        cfg.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, Compatibility.BACKWARD);
        cfg.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, autoRegister);
        // Schema name = topic name: the default naming strategy (AWSSchemaNamingStrategyDefaultImpl) returns the
        // topic name, so no setting is needed. This matches scripts/register-schemas-glue.sh.
        // Default record type; AvroSerdes overrides it to GENERIC_RECORD where we read envelopes / build JSON.
        cfg.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());
        return cfg;
    }
}
