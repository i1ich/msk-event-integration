package io.github.i1ich.mskei.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.i1ich.mskei.schema.AccountEvent;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Config building and factory selection. Building Glue objects does not send any request to AWS. */
class AvroSerdesTest {
    private static final Map<String, String> GLUE_ENV = Map.of(
            "SCHEMA_REGISTRY_TYPE", "glue", "GLUE_REGISTRY_NAME", "dev-msk-ei", "AWS_REGION", "sa-east-1",
            "SCHEMA_AUTO_REGISTER", "false");

    @Test
    void confluentIsTheDefault() {
        Map<String, Object> cfg = SchemaRegistryConfig.from(Map.of("SCHEMA_REGISTRY_URL", "mock://t"));
        assertThat(SchemaRegistryConfig.isGlue(cfg)).isFalse();
        assertThat(AvroSerdes.valueSerializer(cfg)).isInstanceOf(KafkaAvroSerializer.class);
        assertThat(AvroSerdes.genericValueDeserializer(cfg)).isInstanceOf(KafkaAvroDeserializer.class);
        assertThat(AvroSerdes.<AccountEvent>specificValueSerde(cfg).serializer()).isNotNull();
    }

    @Test
    void explicitConfluentTypeIsCaseInsensitive() {
        Map<String, Object> cfg = SchemaRegistryConfig.from(
                Map.of("SCHEMA_REGISTRY_TYPE", "Confluent", "SCHEMA_REGISTRY_URL", "mock://t"));
        assertThat(cfg).containsEntry("schema.registry.url", "mock://t");
    }

    @Test
    void glueConfigHasAllKeys() {
        Map<String, Object> cfg = SchemaRegistryConfig.from(GLUE_ENV);
        assertThat(SchemaRegistryConfig.isGlue(cfg)).isTrue();
        assertThat(cfg)
                .containsEntry(AWSSchemaRegistryConstants.AWS_REGION, "sa-east-1")
                .containsEntry(AWSSchemaRegistryConstants.REGISTRY_NAME, "dev-msk-ei")
                .containsEntry(AWSSchemaRegistryConstants.DATA_FORMAT, "AVRO")
                .containsEntry(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, false)
                .containsKey(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING)
                .doesNotContainKey("schema.registry.url");
        assertThat(cfg.get(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING).toString()).isEqualTo("BACKWARD");
    }

    @Test
    void glueAutoRegisterDefaultsToTrue() {
        Map<String, Object> cfg = SchemaRegistryConfig.from(Map.of(
                "SCHEMA_REGISTRY_TYPE", "glue", "GLUE_REGISTRY_NAME", "r", "AWS_REGION", "us-east-1"));
        assertThat(cfg).containsEntry(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
    }

    @Test
    void glueRecordTypeDependsOnUse() {
        Map<String, Object> cfg = SchemaRegistryConfig.from(GLUE_ENV);
        assertThat(AvroSerdes.glueConfig(cfg, AvroRecordType.GENERIC_RECORD))
                .containsEntry(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, "GENERIC_RECORD");
        assertThat(AvroSerdes.glueConfig(cfg, AvroRecordType.SPECIFIC_RECORD))
                .containsEntry(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, "SPECIFIC_RECORD");
    }

    @Test
    void glueFactoryBuildsGlueObjects() {
        Map<String, Object> cfg = SchemaRegistryConfig.from(GLUE_ENV);
        assertThat(AvroSerdes.valueSerializer(cfg)).isInstanceOf(GlueSchemaRegistryKafkaSerializer.class);
        assertThat(AvroSerdes.genericValueDeserializer(cfg)).isInstanceOf(GlueSchemaRegistryKafkaDeserializer.class);
        assertThat(AvroSerdes.<AccountEvent>specificValueSerde(cfg).serializer()).isNotNull();
    }

    @Test
    void glueNeedsRegistryNameAndRegion() {
        assertThatThrownBy(() -> SchemaRegistryConfig.from(Map.of("SCHEMA_REGISTRY_TYPE", "glue", "AWS_REGION", "r")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("GLUE_REGISTRY_NAME");
        assertThatThrownBy(() -> SchemaRegistryConfig.from(
                Map.of("SCHEMA_REGISTRY_TYPE", "glue", "GLUE_REGISTRY_NAME", "x")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("AWS_REGION");
    }

    @Test
    void unknownTypeIsRejected() {
        assertThatThrownBy(() -> SchemaRegistryConfig.from(Map.of("SCHEMA_REGISTRY_TYPE", "foo")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("foo");
    }
}
