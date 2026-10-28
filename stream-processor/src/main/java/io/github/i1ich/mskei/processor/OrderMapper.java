package io.github.i1ich.mskei.processor;

import io.github.i1ich.mskei.common.AvroSerdes;
import io.github.i1ich.mskei.common.DeadLetters;
import io.github.i1ich.mskei.schema.OrderEvent;
import io.github.i1ich.mskei.schema.OrderOp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Topology B mapping: Debezium envelope (Avro, read as GenericRecord) to {@link OrderEvent}.
 *
 * <p>We deserialize here instead of in a Serde so that a bad payload becomes a normal exception inside the
 * processor and the DLQ gets the ORIGINAL bytes.
 */
public class OrderMapper implements RecordMapper<OrderEvent> {
    /**
     * Deterministic eventId, which becomes the downstream Idempotency-Key.
     *
     * <p>Preferred: the change's position in the PostgreSQL WAL ({@code source.lsn}). It is the same however often the
     * change is re-read: reprocessing, a connector restart, AND a DLQ replay (replay-cli republishes the record at a NEW
     * offset, so an offset-based id would change and the downstream could not detect the duplicate). Snapshot rows share
     * one LSN, so the order id and op are part of the key.
     *
     * <p>Fallback when the envelope has no LSN: the Kafka coordinates (stable for reprocessing, not for replay).
     */
    static String eventId(GenericRecord sourceInfo, long orderId, String op, DeadLetters.Source source) {
        Object lsn = sourceInfo.getSchema().getField("lsn") == null ? null : sourceInfo.get("lsn");
        if (lsn instanceof Number walPosition) {
            return source.topic() + "-" + orderId + "-" + walPosition.longValue() + "-" + op;
        }
        return source.topic() + "-" + source.partition() + "-" + source.offset();
    }

    private static final Logger LOG = LoggerFactory.getLogger(OrderMapper.class);

    private final Deserializer<Object> deserializer;

    /** @param serdeConfig Schema Registry settings (see SchemaRegistryConfig) */
    public OrderMapper(Map<String, ?> serdeConfig) {
        // Generic records: the Debezium envelope has no generated class. Confluent or Glue, chosen by the config.
        this.deserializer = AvroSerdes.genericValueDeserializer(serdeConfig);
    }

    @Override
    public List<Mapped<OrderEvent>> map(byte[] value, Headers headers, DeadLetters.Source source) {
        if (value == null) {
            // Tombstone: Debezium emits it right after a DELETE event only so Kafka log compaction can drop the
            // key. The DELETE event already told us everything, so there is nothing to forward.
            LOG.debug("Skipping tombstone {}-{}-{}", source.topic(), source.partition(), source.offset());
            return List.of();
        }

        GenericRecord envelope = (GenericRecord) deserializer.deserialize(source.topic(), headers, value);
        String op = String.valueOf(envelope.get("op"));
        OrderOp orderOp = switch (op) {
            case "c" -> OrderOp.CREATE;
            case "u" -> OrderOp.UPDATE;
            case "r" -> OrderOp.SNAPSHOT;
            case "d" -> OrderOp.DELETE;
            default -> throw new IllegalArgumentException("Unknown Debezium op '" + op + "'");
        };

        // A delete has no "after" row; the last known state is in "before".
        GenericRecord row = (GenericRecord) envelope.get(orderOp == OrderOp.DELETE ? "before" : "after");
        if (row == null) {
            throw new IllegalArgumentException("Envelope has no row image for op '" + op + "'");
        }
        Object id = row.get("id");
        if (!(id instanceof Number orderId)) {
            throw new IllegalArgumentException("Order row has no numeric id");
        }
        GenericRecord sourceInfo = (GenericRecord) envelope.get("source");
        Object tsMs = sourceInfo.get("ts_ms");
        if (!(tsMs instanceof Number sourceTsMs)) {
            throw new IllegalArgumentException("Envelope source.ts_ms is missing");
        }

        String eventId = eventId(sourceInfo, orderId.longValue(), op, source);
        OrderEvent event = OrderEvent.newBuilder()
                .setEventId(eventId)
                .setOrderId(orderId.longValue())
                .setOp(orderOp)
                .setAccountId(text(row.get("account_id")))
                .setStatus(text(row.get("status")))
                .setAmount(text(row.get("amount")))
                .setCurrency(text(row.get("currency")))
                .setUpdatedAt(toInstant(row.get("updated_at")))
                .setSourceTsMs(sourceTsMs.longValue())
                .build();
        return List.of(new Mapped<>(Long.toString(orderId.longValue()), event));
    }

    private static String text(Object v) {
        return v == null ? null : v.toString();
    }

    /**
     * timestamptz arrives as an ISO-8601 string (Debezium ZonedTimestamp); a plain timestamp as epoch millis.
     * Handle both so a connector setting change does not break us.
     */
    private static Instant toInstant(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Number n) {
            return Instant.ofEpochMilli(n.longValue());
        }
        return Instant.parse(v.toString());
    }
}
