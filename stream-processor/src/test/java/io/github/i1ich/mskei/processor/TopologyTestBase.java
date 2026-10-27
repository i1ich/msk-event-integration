package io.github.i1ich.mskei.processor;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.i1ich.mskei.common.EventHeaders;
import io.github.i1ich.mskei.common.Topics;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/** Shared setup: the real topology on a TopologyTestDriver, with an in-memory ("mock://") Schema Registry. */
abstract class TopologyTestBase {
    static final Map<String, Object> SERDE_CONFIG = Map.of("schema.registry.url", "mock://stream-processor-test");

    TopologyTestDriver driver;
    TestInputTopic<String, byte[]> salesforceIn;
    TestInputTopic<byte[], byte[]> ordersIn;
    TestOutputTopic<byte[], byte[]> dlqOut;

    @BeforeEach
    void startDriver() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        driver = new TopologyTestDriver(ProcessorTopology.build(SERDE_CONFIG), props);
        salesforceIn = driver.createInputTopic(Topics.SF_CHANGE_EVENTS_RAW, Serdes.String().serializer(),
                new ByteArraySerializer());
        ordersIn = driver.createInputTopic(Topics.ERP_ORDERS_CDC, new ByteArraySerializer(),
                new ByteArraySerializer());
        dlqOut = driver.createOutputTopic(Topics.DLQ, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @AfterEach
    void stopDriver() {
        driver.close();
    }

    <V extends SpecificRecord> TestOutputTopic<String, V> output(String topic) {
        SpecificAvroSerde<V> serde = new SpecificAvroSerde<>();
        serde.configure(SERDE_CONFIG, false);
        return driver.createOutputTopic(topic, new StringDeserializer(), serde.deserializer());
    }

    static Headers headers(String eventId) {
        Headers h = new RecordHeaders();
        h.add(EventHeaders.EVENT_ID, eventId.getBytes(StandardCharsets.UTF_8));
        h.add(EventHeaders.SOURCE, "salesforce".getBytes(StandardCharsets.UTF_8));
        return h;
    }

    static TestRecord<String, byte[]> sf(String key, String json, Headers headers) {
        return new TestRecord<>(key, json.getBytes(StandardCharsets.UTF_8), headers, 1_000L);
    }

    static String header(TestRecord<?, ?> record, String name) {
        var h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
