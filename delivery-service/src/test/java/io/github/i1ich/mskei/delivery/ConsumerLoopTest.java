package io.github.i1ich.mskei.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.i1ich.mskei.common.Topics;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConsumerLoopTest {
    /** A clock the test can move. */
    static final class TestClock extends Clock {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    private final TestClock clock = new TestClock();
    private final AtomicInteger posts = new AtomicInteger();
    private final AtomicInteger status = new AtomicInteger(200);
    private final MockProducer<byte[], byte[]> producer =
            new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
    private final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private RecordHandler handler;

    @BeforeEach
    void setUp() {
        DownstreamSender sender = (path, key, json) -> {
            posts.incrementAndGet();
            return status.get();
        };
        handler = new RecordHandler(TestEvents.deserializer(), sender, producer,
                new RetryRouter(clock, Duration.ofMinutes(1), Duration.ofMinutes(10)));
    }

    private TopicPartition assign(String topic) {
        TopicPartition tp = new TopicPartition(topic, 0);
        consumer.assign(List.of(tp));
        consumer.updateBeginningOffsets(Map.of(tp, 0L));
        return tp;
    }

    private ConsumerRecord<byte[], byte[]> rec(String topic, long offset, String notBefore) {
        ConsumerRecord<byte[], byte[]> r = new ConsumerRecord<>(topic, 0, offset,
                "k".getBytes(StandardCharsets.UTF_8), TestEvents.account("e" + offset, "001A"));
        r.headers().add("source-topic", Topics.ACCOUNT_V1.getBytes(StandardCharsets.UTF_8));
        if (notBefore != null) {
            r.headers().add("not-before", notBefore.getBytes(StandardCharsets.UTF_8));
        }
        return r;
    }

    @Test
    void deliversThenCommitsNextOffset() {
        TopicPartition tp = assign(Topics.ACCOUNT_V1);
        consumer.addRecord(rec(Topics.ACCOUNT_V1, 0, null));
        consumer.addRecord(rec(Topics.ACCOUNT_V1, 1, null));
        ConsumerLoop loop = new ConsumerLoop("main", consumer, List.of(Topics.ACCOUNT_V1), handler, false, clock);

        loop.pollOnce();

        assertThat(posts.get()).isEqualTo(2);
        assertThat(consumer.committed(Set.of(tp)).get(tp)).isEqualTo(new OffsetAndMetadata(2));
    }

    @Test
    void failedRecordIsWrittenToRetryTopicBeforeCommit() {
        TopicPartition tp = assign(Topics.ACCOUNT_V1);
        status.set(503);
        consumer.addRecord(rec(Topics.ACCOUNT_V1, 0, null));
        ConsumerLoop loop = new ConsumerLoop("main", consumer, List.of(Topics.ACCOUNT_V1), handler, false, clock);

        loop.pollOnce();

        assertThat(producer.history()).hasSize(1);
        assertThat(producer.history().get(0).topic()).isEqualTo(Topics.RETRY_1M);
        assertThat(consumer.committed(Set.of(tp)).get(tp)).isEqualTo(new OffsetAndMetadata(1));
    }

    @Test
    void notDueHeadRecordPausesPartitionUntilDue() {
        TopicPartition tp = assign(Topics.RETRY_1M);
        consumer.addRecord(rec(Topics.RETRY_1M, 0, "2026-01-01T00:01:00Z"));
        consumer.addRecord(rec(Topics.RETRY_1M, 1, "2026-01-01T00:01:01Z"));
        ConsumerLoop loop = new ConsumerLoop("retry", consumer, List.of(Topics.RETRY_1M), handler, true, clock);

        loop.pollOnce();

        assertThat(posts.get()).isZero();
        assertThat(consumer.paused()).containsExactly(tp);
        assertThat(consumer.position(tp)).isEqualTo(0L);
        assertThat(consumer.committed(Set.of(tp)).get(tp)).isNull();

        loop.pollOnce(); // still too early: stays paused
        assertThat(consumer.paused()).containsExactly(tp);

        clock.now = Instant.parse("2026-01-01T00:01:00Z");
        loop.pollOnce();
        assertThat(consumer.paused()).isEmpty();
    }

    @Test
    void dueRecordInRetryTopicIsDelivered() {
        TopicPartition tp = assign(Topics.RETRY_1M);
        consumer.addRecord(rec(Topics.RETRY_1M, 0, "2025-12-31T23:59:00Z"));
        ConsumerLoop loop = new ConsumerLoop("retry", consumer, List.of(Topics.RETRY_1M), handler, true, clock);

        loop.pollOnce();

        assertThat(posts.get()).isEqualTo(1);
        assertThat(consumer.committed(Set.of(tp)).get(tp)).isEqualTo(new OffsetAndMetadata(1));
    }

    @Test
    void revokedPartitionsAreForgotten() {
        TopicPartition tp = assign(Topics.RETRY_1M);
        consumer.addRecord(rec(Topics.RETRY_1M, 0, "2026-01-01T00:01:00Z"));
        ConsumerLoop loop = new ConsumerLoop("retry", consumer, List.of(Topics.RETRY_1M), handler, true, clock);
        loop.pollOnce();

        loop.onPartitionsRevoked(List.of(tp));
        loop.onPartitionsAssigned(List.of(tp));
        clock.now = Instant.parse("2026-01-01T01:00:00Z");
        loop.pollOnce();

        // The loop no longer remembers the pause (a real rebalance clears pause state on the consumer itself).
        assertThat(consumer.paused()).containsExactly(tp);
    }

    @Test
    void wakeupEndsRunAndClosesConsumer() {
        MockConsumer<byte[], byte[]> c = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        c.schedulePollTask(c::wakeup);
        ConsumerLoop loop = new ConsumerLoop("main", c, List.of(Topics.ACCOUNT_V1), handler, false, clock);

        loop.run();

        assertThat(c.closed()).isTrue();
    }

    @Test
    void stopCallsWakeup() {
        ConsumerLoop loop = new ConsumerLoop("main", consumer, List.of(Topics.ACCOUNT_V1), handler, false, clock);
        loop.stop();
        org.junit.jupiter.api.Assertions.assertThrows(org.apache.kafka.common.errors.WakeupException.class,
                () -> consumer.poll(Duration.ZERO));
    }
}
