package io.github.i1ich.mskei.delivery;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.i1ich.mskei.common.Topics;
import io.github.i1ich.mskei.schema.AccountEvent;
import io.github.i1ich.mskei.schema.ChangeType;
import io.github.i1ich.mskei.schema.OrderEvent;
import io.github.i1ich.mskei.schema.OrderOp;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Builds Avro-encoded record values with an in-memory ("mock://") Schema Registry. */
final class TestEvents {
    static final Map<String, Object> REGISTRY = Map.of(
            "schema.registry.url", "mock://delivery-tests",
            "auto.register.schemas", true);

    private static final KafkaAvroSerializer SERIALIZER = new KafkaAvroSerializer();

    static {
        SERIALIZER.configure(REGISTRY, false);
    }

    private TestEvents() {
    }

    static KafkaAvroDeserializer deserializer() {
        KafkaAvroDeserializer d = new KafkaAvroDeserializer();
        d.configure(REGISTRY, false);
        return d;
    }

    static byte[] account(String eventId, String accountId) {
        AccountEvent e = AccountEvent.newBuilder()
                .setEventId(eventId)
                .setAccountId(accountId)
                .setChangeType(ChangeType.UPDATE)
                .setName("Acme \"quoted\"")
                .setCommitTimestamp(Instant.parse("2026-01-01T00:00:00Z"))
                .build();
        return SERIALIZER.serialize(Topics.ACCOUNT_V1, e);
    }

    static byte[] order(String eventId, String accountId) {
        OrderEvent e = OrderEvent.newBuilder()
                .setEventId(eventId)
                .setOrderId(42L)
                .setOp(OrderOp.CREATE)
                .setAccountId(accountId)
                .setSourceTsMs(1L)
                .build();
        return SERIALIZER.serialize(Topics.ORDER_V1, e);
    }

    static ConsumerRecord<byte[], byte[]> record(String topic, byte[] value) {
        return new ConsumerRecord<>(topic, 0, 7L, "k".getBytes(StandardCharsets.UTF_8), value);
    }

    static ConsumerRecord<byte[], byte[]> retryRecord(String topic, String sourceTopic, byte[] value) {
        ConsumerRecord<byte[], byte[]> r = new ConsumerRecord<>(topic, 0, 3L,
                "k".getBytes(StandardCharsets.UTF_8), value);
        r.headers().add("source-topic", sourceTopic.getBytes(StandardCharsets.UTF_8));
        return r;
    }

    static String header(ProducerRecord<byte[], byte[]> r, String name) {
        var h = r.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
