package io.github.i1ich.mskei.processor;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.i1ich.mskei.common.EventHeaders;
import io.github.i1ich.mskei.common.Topics;
import io.github.i1ich.mskei.schema.OrderEvent;
import io.github.i1ich.mskei.schema.OrderOp;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

class OrderTopologyTest extends TopologyTestBase {

    /** Mirrors the shape of Debezium's Avro envelope for public.orders (only the fields we read plus a few). */
    private static final Schema ENVELOPE = new Schema.Parser().parse("""
            {"type":"record","name":"Envelope","namespace":"erp.public.orders","fields":[
              {"name":"before","type":["null",{"type":"record","name":"Value","fields":[
                {"name":"id","type":"int"},
                {"name":"account_id","type":["null","string"],"default":null},
                {"name":"status","type":["null","string"],"default":null},
                {"name":"amount","type":["null","string"],"default":null},
                {"name":"currency","type":["null","string"],"default":null},
                {"name":"updated_at","type":["null","string","long"],"default":null}]}],"default":null},
              {"name":"after","type":["null","Value"],"default":null},
              {"name":"source","type":{"type":"record","name":"Source","fields":[
                {"name":"ts_ms","type":"long"}]}},
              {"name":"op","type":"string"}]}""");
    private static final Schema VALUE = ENVELOPE.getField("after").schema().getTypes().get(1);
    private static final Schema SOURCE = ENVELOPE.getField("source").schema();

    private final KafkaAvroSerializer serializer = serializer();

    private static KafkaAvroSerializer serializer() {
        KafkaAvroSerializer s = new KafkaAvroSerializer();
        s.configure(SERDE_CONFIG, false);
        return s;
    }

    private static GenericRecord row(Object updatedAt) {
        GenericRecord r = new GenericData.Record(VALUE);
        r.put("id", 1);
        r.put("account_id", "001000000000001AAA");
        r.put("status", "NEW");
        r.put("amount", "120.50");
        r.put("currency", "EUR");
        r.put("updated_at", updatedAt);
        return r;
    }

    private byte[] envelope(String op, GenericRecord before, GenericRecord after) {
        GenericRecord source = new GenericData.Record(SOURCE);
        source.put("ts_ms", 1793127631566L);
        GenericRecord env = new GenericData.Record(ENVELOPE);
        env.put("before", before);
        env.put("after", after);
        env.put("source", source);
        env.put("op", op);
        return serializer.serialize(Topics.ERP_ORDERS_CDC, env);
    }

    private static byte[] key(int id) {
        return Integer.toString(id).getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void createIsMapped() {
        TestOutputTopic<String, OrderEvent> out = output(Topics.ORDER_V1);

        ordersIn.pipeInput(key(1), envelope("c", null, row("2026-10-27T18:55:40.082814Z")));

        TestRecord<String, OrderEvent> r = out.readRecord();
        assertThat(r.key()).isEqualTo("1");
        assertThat(r.value()).isEqualTo(OrderEvent.newBuilder()
                .setEventId("erp.public.orders-0-0").setOrderId(1L).setOp(OrderOp.CREATE)
                .setAccountId("001000000000001AAA").setStatus("NEW").setAmount("120.50").setCurrency("EUR")
                .setUpdatedAt(Instant.parse("2026-10-27T18:55:40.082814Z").truncatedTo(
                        java.time.temporal.ChronoUnit.MILLIS))
                .setSourceTsMs(1793127631566L).build());
        assertThat(dlqOut.isEmpty()).isTrue();
    }

    @Test
    void updateAndSnapshotMapTheirOps() {
        TestOutputTopic<String, OrderEvent> out = output(Topics.ORDER_V1);

        ordersIn.pipeInput(key(1), envelope("u", row("2026-10-27T18:55:40Z"), row("2026-10-27T18:56:00Z")));
        ordersIn.pipeInput(key(1), envelope("r", null, row("2026-10-27T18:55:40Z")));

        assertThat(out.readValuesToList()).extracting(OrderEvent::getOp)
                .containsExactly(OrderOp.UPDATE, OrderOp.SNAPSHOT);
    }

    @Test
    void deleteTakesFieldsFromBeforeImage() {
        TestOutputTopic<String, OrderEvent> out = output(Topics.ORDER_V1);

        ordersIn.pipeInput(key(1), envelope("d", row("2026-10-27T18:55:40Z"), null));

        OrderEvent e = out.readValue();
        assertThat(e.getOp()).isEqualTo(OrderOp.DELETE);
        assertThat(e.getOrderId()).isEqualTo(1L);
        assertThat(e.getStatus()).isEqualTo("NEW");
    }

    @Test
    void eventIdIsDeterministicFromPartitionAndOffset() {
        TestOutputTopic<String, OrderEvent> out = output(Topics.ORDER_V1);

        ordersIn.pipeInput(key(1), envelope("c", null, row(null)));
        ordersIn.pipeInput(key(1), envelope("u", null, row(null)));

        assertThat(out.readValuesToList()).extracting(OrderEvent::getEventId)
                .containsExactly("erp.public.orders-0-0", "erp.public.orders-0-1");
    }

    @Test
    void epochMillisUpdatedAtAndMissingUpdatedAtAreHandled() {
        TestOutputTopic<String, OrderEvent> out = output(Topics.ORDER_V1);

        ordersIn.pipeInput(key(1), envelope("c", null, row(1793127631566L)));
        ordersIn.pipeInput(key(1), envelope("c", null, row(null)));

        var events = out.readValuesToList();
        assertThat(events.get(0).getUpdatedAt()).isEqualTo(Instant.ofEpochMilli(1793127631566L));
        assertThat(events.get(1).getUpdatedAt()).isNull();
    }

    @Test
    void tombstoneIsSkippedWithoutOutputOrDlq() {
        TestOutputTopic<String, OrderEvent> out = output(Topics.ORDER_V1);

        ordersIn.pipeInput(new TestRecord<byte[], byte[]>(key(1), null));

        assertThat(out.isEmpty()).isTrue();
        assertThat(dlqOut.isEmpty()).isTrue();
    }

    @Test
    void garbageBytesGoToDlqWithOriginalBytes() {
        byte[] garbage = {1, 2, 3, 4, 5, 6, 7};

        ordersIn.pipeInput(new TestRecord<>(key(7), garbage, headers("x"), 1L));

        TestRecord<byte[], byte[]> dlq = dlqOut.readRecord();
        assertThat(dlq.value()).isEqualTo(garbage);
        assertThat(dlq.key()).isEqualTo(key(7));
        assertThat(header(dlq, EventHeaders.SOURCE_TOPIC)).isEqualTo(Topics.ERP_ORDERS_CDC);
        assertThat(header(dlq, EventHeaders.ATTEMPTS)).isEqualTo("1");
        assertThat(header(dlq, EventHeaders.ERROR_CLASS)).isNotBlank();
        assertThat(header(dlq, EventHeaders.EVENT_ID)).isEqualTo("x");
        assertThat(output(Topics.ORDER_V1).isEmpty()).isTrue();
    }

    @Test
    void unknownOpAndMissingImagesGoToDlq() {
        ordersIn.pipeInput(key(1), envelope("x", null, row(null)));
        ordersIn.pipeInput(key(1), envelope("c", null, null));
        ordersIn.pipeInput(key(1), envelope("d", null, null));

        var dlq = dlqOut.readRecordsToList();
        assertThat(dlq).hasSize(3);
        assertThat(header(dlq.get(0), EventHeaders.ERROR_MESSAGE)).contains("Unknown Debezium op");
        assertThat(output(Topics.ORDER_V1).isEmpty()).isTrue();
    }

    @Test
    void avroValueThatIsNotADebeziumEnvelopeGoesToDlq() {
        // Valid Avro, but no "op" field: the typical symptom of a schema mismatch.
        Schema other = new Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"Other\",\"fields\":[{\"name\":\"hello\",\"type\":\"string\"}]}");
        GenericRecord weird = new GenericData.Record(other);
        weird.put("hello", "world");

        ordersIn.pipeInput(key(1), serializer.serialize(Topics.ERP_ORDERS_CDC, weird));

        assertThat(dlqOut.readRecord()).isNotNull();
    }
}
