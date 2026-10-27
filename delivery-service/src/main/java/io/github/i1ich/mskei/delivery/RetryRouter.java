package io.github.i1ich.mskei.delivery;

import io.github.i1ich.mskei.common.DeadLetters;
import io.github.i1ich.mskei.common.EventHeaders;
import io.github.i1ich.mskei.common.Topics;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

/**
 * Decides where a failed record goes next. The ladder (3 attempts in total):
 * <pre>
 *   attempt 1 (main topic)  fails -> integration.retry.1m
 *   attempt 2 (retry.1m)    fails -> integration.retry.10m
 *   attempt 3 (retry.10m)   fails -> integration.dlq
 * </pre>
 * The topic names are fixed; only the delay stored in {@code not-before} is configurable.
 */
public final class RetryRouter {
    public static final int MAX_ATTEMPTS = 3;

    private static final Set<String> OWN_HEADERS = Set.of(
            EventHeaders.SOURCE_TOPIC, EventHeaders.ATTEMPTS, EventHeaders.NOT_BEFORE);

    private final Clock clock;
    private final Duration delay1m;
    private final Duration delay10m;

    public RetryRouter(Clock clock, Duration delay1m, Duration delay10m) {
        this.clock = clock;
        this.delay1m = delay1m;
        this.delay10m = delay10m;
    }

    /** Which delivery attempt a record read from this topic represents (1, 2 or 3). */
    public static int attemptOf(String topic) {
        return switch (topic) {
            case Topics.RETRY_1M -> 2;
            case Topics.RETRY_10M -> MAX_ATTEMPTS;
            default -> 1;
        };
    }

    public static boolean isRetryTopic(String topic) {
        return Topics.RETRY_1M.equals(topic) || Topics.RETRY_10M.equals(topic);
    }

    /** The record to write after a RETRYABLE failure: next rung of the ladder, or the DLQ. */
    public ProducerRecord<byte[], byte[]> onRetryable(ConsumerRecord<byte[], byte[]> failed,
            String originalTopic, Throwable error) {
        return switch (failed.topic()) {
            case Topics.RETRY_1M -> retry(failed, originalTopic, Topics.RETRY_10M, delay10m, 2);
            case Topics.RETRY_10M -> onPermanent(failed, originalTopic, error);
            default -> retry(failed, originalTopic, Topics.RETRY_1M, delay1m, 1);
        };
    }

    /** The DLQ record for a failure that will not be retried (or that used up its attempts). */
    public ProducerRecord<byte[], byte[]> onPermanent(ConsumerRecord<byte[], byte[]> failed,
            String originalTopic, Throwable error) {
        // Source topic is the canonical topic so a replay knows where to republish; partition and offset
        // are those of the record where it failed (they point into the retry topic after a retry).
        DeadLetters.Source source = new DeadLetters.Source(originalTopic, failed.partition(), failed.offset());
        return DeadLetters.record(source, failed.key(), failed.value(), failed.headers(), error,
                attemptOf(failed.topic()), clock.instant());
    }

    private ProducerRecord<byte[], byte[]> retry(ConsumerRecord<byte[], byte[]> failed, String originalTopic,
            String targetTopic, Duration delay, int attemptsSoFar) {
        // Same key (same partition per key, so per-entity order is kept) and the ORIGINAL value bytes.
        ProducerRecord<byte[], byte[]> out = new ProducerRecord<>(targetTopic, null, failed.key(), failed.value());
        for (Header h : failed.headers()) {
            if (!OWN_HEADERS.contains(h.key())) {
                out.headers().add(h.key(), h.value());
            }
        }
        add(out, EventHeaders.SOURCE_TOPIC, originalTopic);
        add(out, EventHeaders.ATTEMPTS, Integer.toString(attemptsSoFar));
        add(out, EventHeaders.NOT_BEFORE, clock.instant().plus(delay).toString());
        return out;
    }

    private static void add(ProducerRecord<byte[], byte[]> r, String name, String value) {
        r.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }
}
