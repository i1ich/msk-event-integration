package io.github.i1ich.mskei.replay;

import io.github.i1ich.mskei.common.EventHeaders;
import io.github.i1ich.mskei.common.Topics;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/** Builds a MockConsumer holding a one-partition DLQ, plus record helpers for tests. */
final class DlqFixture {
    static final TopicPartition TP = new TopicPartition(Topics.DLQ, 0);

    private DlqFixture() {
    }

    static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** A DLQ record at the given offset. Pass null for sourceTopic/errorClass/failedAt to omit that header. */
    static ConsumerRecord<byte[], byte[]> dlq(long offset, String key, String value, String sourceTopic,
            String errorClass, String failedAt) {
        ConsumerRecord<byte[], byte[]> r = new ConsumerRecord<>(Topics.DLQ, 0, offset, b(key), b(value));
        r.headers().add(EventHeaders.EVENT_ID, b("evt-" + offset));
        r.headers().add(EventHeaders.ERROR_MESSAGE, b("boom"));
        r.headers().add(EventHeaders.SOURCE_PARTITION, b("3"));
        r.headers().add(EventHeaders.SOURCE_OFFSET, b("42"));
        r.headers().add(EventHeaders.ATTEMPTS, b("3"));
        r.headers().add(EventHeaders.NOT_BEFORE, b("2026-01-01T00:00:00Z"));
        if (sourceTopic != null) {
            r.headers().add(EventHeaders.SOURCE_TOPIC, b(sourceTopic));
        }
        if (errorClass != null) {
            r.headers().add(EventHeaders.ERROR_CLASS, b(errorClass));
        }
        if (failedAt != null) {
            r.headers().add(EventHeaders.FAILED_AT, b(failedAt));
        }
        return r;
    }

    /**
     * Consumer whose log end is {@code endOffset}; all given records are delivered by the first poll
     * (so a test can put records beyond the end offset to simulate late arrivals).
     */
    static MockConsumer<byte[], byte[]> consumer(long endOffset, List<ConsumerRecord<byte[], byte[]>> records) {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updatePartitions(Topics.DLQ, List.of(new PartitionInfo(Topics.DLQ, 0, null, null, null)));
        consumer.updateBeginningOffsets(Map.of(TP, 0L));
        consumer.updateEndOffsets(Map.of(TP, endOffset));
        // addRecord needs an assigned partition, so deliver inside the first poll
        consumer.schedulePollTask(() -> new ArrayList<>(records).forEach(consumer::addRecord));
        return consumer;
    }
}
