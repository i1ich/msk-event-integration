package io.github.i1ich.mskei.processor;

import io.github.i1ich.mskei.common.AvroSerdes;
import io.github.i1ich.mskei.common.Topics;
import io.github.i1ich.mskei.schema.AccountEvent;
import io.github.i1ich.mskei.schema.OrderEvent;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.avro.specific.SpecificRecord;

/**
 * Builds both topologies in one {@link Topology} (one app, one consumer group, one transaction scope).
 *
 * <pre>
 *  A: sf.change-events.raw --[SafeMapProcessor(AccountMapper)]--+--> ok     --> integration.account.v1
 *                                                               +--> failed --> integration.dlq
 *  B: erp.public.orders    --[SafeMapProcessor(OrderMapper)]----+--> ok     --> integration.order.v1
 *                                                               +--> failed --> integration.dlq
 * </pre>
 *
 * Records are keyed by account id / order id on output, so all events of one entity land on one partition and
 * stay ordered for the delivery service.
 */
public final class ProcessorTopology {
    private ProcessorTopology() {
    }

    /** @param serdeConfig Schema Registry settings for Avro serdes */
    public static Topology build(Map<String, ?> serdeConfig) {
        StreamsBuilder builder = new StreamsBuilder();

        // A: Salesforce JSON. Value stays raw bytes so the DLQ can keep the original payload.
        KStream<String, byte[]> sf = builder.stream(Topics.SF_CHANGE_EVENTS_RAW,
                Consumed.with(Serdes.String(), Serdes.ByteArray()));
        route(sf, new AccountMapper(), key -> key.getBytes(StandardCharsets.UTF_8),
                Topics.ACCOUNT_V1, AccountEvent.class, serdeConfig);

        // B: Debezium Avro. Key and value both raw bytes; the mapper deserializes the value itself.
        KStream<byte[], byte[]> erp = builder.stream(Topics.ERP_ORDERS_CDC,
                Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()));
        route(erp, new OrderMapper(serdeConfig), Function.identity(),
                Topics.ORDER_V1, OrderEvent.class, serdeConfig);

        return builder.build();
    }

    private static <K, V extends SpecificRecord> void route(KStream<K, byte[]> input, RecordMapper<V> mapper,
            Function<K, byte[]> keyBytes, String outputTopic, Class<V> type, Map<String, ?> serdeConfig) {
        Serde<V> valueSerde = AvroSerdes.specificValueSerde(serdeConfig);

        // process() = the new Processor API; one Result per output record (see SafeMapProcessor).
        KStream<String, Result<V>> results = input.process(() -> new SafeMapProcessor<K, V>(mapper, keyBytes));

        results.split(Named.as(outputTopic + "-"))
                .branch((key, result) -> result.isFailed(), Branched.withConsumer(failed -> failed
                        // Headers of the forwarded record are already the DLQ headers; map() keeps them.
                        .map((key, result) -> KeyValue.pair(result.deadLetter().key(), result.deadLetter().value()))
                        .to(Topics.DLQ, Produced.with(Serdes.ByteArray(), Serdes.ByteArray()))))
                .defaultBranch(Branched.withConsumer(ok -> ok
                        .mapValues(Result::value)
                        .to(outputTopic, Produced.with(Serdes.String(), valueSerde))));
    }
}
