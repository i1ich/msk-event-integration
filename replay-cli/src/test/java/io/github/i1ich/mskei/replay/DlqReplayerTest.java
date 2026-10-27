package io.github.i1ich.mskei.replay;

import static io.github.i1ich.mskei.replay.DlqFixture.b;
import static io.github.i1ich.mskei.replay.DlqFixture.dlq;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.i1ich.mskei.common.DeadLetters;
import io.github.i1ich.mskei.common.EventHeaders;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

class DlqReplayerTest {
    private static final String ERR = "io.github.i1ich.mskei.stream.UnmappedEntityException";
    private static final String OK_TIME = "2026-01-01T10:30:00Z";

    private final MockProducer<byte[], byte[]> producer =
            new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
    private final ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
    private final PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
    private int sleepCalls = 0;
    /** Fixed clock and a sleeper that only counts: tests never really sleep. */
    private final RateLimiter limiter = new RateLimiter(10, () -> 0L, n -> sleepCalls++);

    private DlqReplayer.Summary run(MockConsumer<byte[], byte[]> consumer, ReplayFilter filter, Long max,
            boolean dryRun) throws Exception {
        return new DlqReplayer(consumer, producer, filter, limiter, max, dryRun, out).run();
    }

    private static ReplayFilter all() {
        return new ReplayFilter(null, null, null, null);
    }

    private static ConsumerRecord<byte[], byte[]> rec(long offset, String topic, String err) {
        return dlq(offset, "key-" + offset, "value-" + offset, topic, err, OK_TIME);
    }

    @Test
    void replaysToSourceTopicWithSameBytesAndCleanHeaders() throws Exception {
        MockConsumer<byte[], byte[]> c = DlqFixture.consumer(1, List.of(rec(0, "integration.account.v1", ERR)));

        DlqReplayer.Summary s = run(c, all(), null, false);

        assertThat(s).isEqualTo(new DlqReplayer.Summary(1, 1, 1, 0, 0));
        assertThat(producer.history()).hasSize(1);
        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertThat(sent.topic()).isEqualTo("integration.account.v1");
        assertThat(sent.key()).isEqualTo(b("key-0"));
        assertThat(sent.value()).isEqualTo(b("value-0"));
        // original header kept
        assertThat(DeadLetters.header(sent.headers(), EventHeaders.EVENT_ID)).isEqualTo("evt-0");
        // all error headers removed
        for (String name : List.of(EventHeaders.ERROR_CLASS, EventHeaders.ERROR_MESSAGE, EventHeaders.SOURCE_TOPIC,
                EventHeaders.SOURCE_PARTITION, EventHeaders.SOURCE_OFFSET, EventHeaders.FAILED_AT,
                EventHeaders.ATTEMPTS, EventHeaders.NOT_BEFORE)) {
            assertThat(sent.headers().lastHeader(name)).as(name).isNull();
        }
        // replayed-at added and is a valid instant
        Header replayedAt = sent.headers().lastHeader(EventHeaders.REPLAYED_AT);
        assertThat(replayedAt).isNotNull();
        assertThat(Instant.parse(new String(replayedAt.value(), StandardCharsets.UTF_8))).isNotNull();
    }

    @Test
    void filtersBySourceTopicAndErrorClass() throws Exception {
        MockConsumer<byte[], byte[]> c = DlqFixture.consumer(4, List.of(
                rec(0, "a", ERR), rec(1, "b", ERR), rec(2, "a", "java.lang.IllegalStateException"), rec(3, "a", ERR)));

        DlqReplayer.Summary s = run(c, new ReplayFilter("a", "UnmappedEntityException", null, null), null, false);

        assertThat(s).isEqualTo(new DlqReplayer.Summary(4, 2, 2, 0, 0));
        assertThat(producer.history()).extracting(r -> new String(r.key(), StandardCharsets.UTF_8))
                .containsExactly("key-0", "key-3");
    }

    @Test
    void filtersByTimeRange() throws Exception {
        MockConsumer<byte[], byte[]> c = DlqFixture.consumer(2, List.of(
                dlq(0, "k0", "v0", "a", ERR, "2026-01-01T09:00:00Z"),
                dlq(1, "k1", "v1", "a", ERR, "2026-01-01T10:30:00Z")));
        ReplayFilter f = new ReplayFilter(null, null, Instant.parse("2026-01-01T10:00:00Z"),
                Instant.parse("2026-01-01T11:00:00Z"));

        DlqReplayer.Summary s = run(c, f, null, false);

        assertThat(s.matched()).isEqualTo(1);
        assertThat(producer.history().get(0).key()).isEqualTo(b("k1"));
    }

    @Test
    void dryRunPublishesNothingButReportsMatches() throws Exception {
        MockConsumer<byte[], byte[]> c = DlqFixture.consumer(2, List.of(rec(0, "a", ERR), rec(1, "b", ERR)));

        DlqReplayer.Summary s = run(c, new ReplayFilter("a", null, null, null), null, true);

        assertThat(producer.history()).isEmpty();
        assertThat(s).isEqualTo(new DlqReplayer.Summary(2, 1, 0, 0, 0));
        assertThat(outBytes.toString(StandardCharsets.UTF_8))
                .contains("offset=0", "source-topic=a", "error-class=" + ERR, "event-id=evt-0", "key=key-0")
                .doesNotContain("offset=1");
        assertThat(sleepCalls).isZero();
    }

    @Test
    void skipsRecordsWithoutSourceTopicAndCountsThem() throws Exception {
        MockConsumer<byte[], byte[]> c = DlqFixture.consumer(2, List.of(rec(0, null, ERR), rec(1, "a", ERR)));

        DlqReplayer.Summary s = run(c, all(), null, false);

        assertThat(s).isEqualTo(new DlqReplayer.Summary(2, 1, 1, 1, 0));
        assertThat(producer.history()).hasSize(1);
    }

    @Test
    void maxStopsAfterNMatches() throws Exception {
        MockConsumer<byte[], byte[]> c = DlqFixture.consumer(5, List.of(
                rec(0, "a", ERR), rec(1, "a", ERR), rec(2, "a", ERR), rec(3, "a", ERR), rec(4, "a", ERR)));

        DlqReplayer.Summary s = run(c, all(), 2L, false);

        assertThat(s.replayed()).isEqualTo(2);
        assertThat(producer.history()).hasSize(2);
        assertThat(s.scanned()).isEqualTo(2);
    }

    @Test
    void doesNotReadRecordsThatArrivedAfterStart() throws Exception {
        // log end was 2 when we started; offsets 2 and 3 are "new arrivals"
        MockConsumer<byte[], byte[]> c = DlqFixture.consumer(2, List.of(
                rec(0, "a", ERR), rec(1, "a", ERR), rec(2, "a", ERR), rec(3, "a", ERR)));

        DlqReplayer.Summary s = run(c, all(), null, false);

        assertThat(s.scanned()).isEqualTo(2);
        assertThat(producer.history()).hasSize(2);
    }

    @Test
    void emptyDlqFinishesImmediately() throws Exception {
        MockConsumer<byte[], byte[]> c = DlqFixture.consumer(0, List.of());

        DlqReplayer.Summary s = run(c, all(), null, false);

        assertThat(s).isEqualTo(new DlqReplayer.Summary(0, 0, 0, 0, 0));
    }

    @Test
    void usesRateLimiterBetweenSends() throws Exception {
        MockConsumer<byte[], byte[]> c = DlqFixture.consumer(3, List.of(
                rec(0, "a", ERR), rec(1, "a", ERR), rec(2, "a", ERR)));
        // fixed clock => every acquire after the first has to sleep
        run(c, all(), null, false);
        assertThat(sleepCalls).isEqualTo(2);
    }

    @Test
    void countsFailedSends() throws Exception {
        MockProducer<byte[], byte[]> failing = new MockProducer<>(true, new ByteArraySerializer(),
                new ByteArraySerializer()) {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> r, Callback callback) {
                callback.onCompletion(null, new RuntimeException("broker down"));
                return null;
            }
        };
        MockConsumer<byte[], byte[]> c = DlqFixture.consumer(2, List.of(rec(0, "a", ERR), rec(1, "a", ERR)));

        DlqReplayer.Summary s = new DlqReplayer(c, failing, all(), limiter, null, false, out).run();

        assertThat(s.failed()).isEqualTo(2);
    }

    @Test
    void doesNotCommitOffsets() throws Exception {
        MockConsumer<byte[], byte[]> c = DlqFixture.consumer(1, List.of(rec(0, "a", ERR)));
        run(c, all(), null, false);
        assertThat(c.committed(Set.of(DlqFixture.TP)).get(DlqFixture.TP)).isNull();
    }
}
