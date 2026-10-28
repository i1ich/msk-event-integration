package io.github.i1ich.mskei.common;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.kafkastreams.GlueSchemaRegistryKafkaStreamsSerde;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * One place that hides "Confluent or Glue". Callers pass the map from {@link SchemaRegistryConfig} and get
 * ready-to-use (already configured) Kafka serializers / serdes. Creating them does not call the registry or AWS;
 * the first serialize/deserialize does.
 */
public final class AvroSerdes {
    private AvroSerdes() {
    }

    /** Value serializer for Avro specific records (our generated classes). */
    public static Serializer<Object> valueSerializer(Map<String, ?> cfg) {
        if (SchemaRegistryConfig.isGlue(cfg)) {
            return new GlueSchemaRegistryKafkaSerializer(glueConfig(cfg, AvroRecordType.SPECIFIC_RECORD));
        }
        KafkaAvroSerializer s = new KafkaAvroSerializer();
        s.configure(cfg, false);
        return s;
    }

    /** Value deserializer that returns GenericRecord (Debezium envelopes, delivery JSON body). */
    public static Deserializer<Object> genericValueDeserializer(Map<String, ?> cfg) {
        if (SchemaRegistryConfig.isGlue(cfg)) {
            return new GlueSchemaRegistryKafkaDeserializer(glueConfig(cfg, AvroRecordType.GENERIC_RECORD));
        }
        KafkaAvroDeserializer d = new KafkaAvroDeserializer();
        d.configure(cfg, false);
        return d;
    }

    /** Kafka Streams value serde for an output topic that carries one generated Avro class. */
    @SuppressWarnings("unchecked")
    public static <T extends SpecificRecord> Serde<T> specificValueSerde(Map<String, ?> cfg) {
        if (SchemaRegistryConfig.isGlue(cfg)) {
            GlueSchemaRegistryKafkaStreamsSerde serde = new GlueSchemaRegistryKafkaStreamsSerde();
            serde.configure(glueConfig(cfg, AvroRecordType.SPECIFIC_RECORD), false);
            // The Glue serde is typed Serde<Object>; it handles any Avro record, so the cast is safe.
            return (Serde<T>) (Serde<?>) serde;
        }
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(cfg, false);
        return serde;
    }

    /** Copy of the Glue config with the Avro record type set for this use. */
    static Map<String, Object> glueConfig(Map<String, ?> cfg, AvroRecordType type) {
        Map<String, Object> copy = new HashMap<>(cfg);
        copy.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, type.getName());
        return copy;
    }
}
