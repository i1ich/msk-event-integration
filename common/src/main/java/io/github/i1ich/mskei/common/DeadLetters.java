package io.github.i1ich.mskei.common;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Set;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * Builds records for {@link Topics#DLQ}. Contract: key and value are the ORIGINAL bytes (so replay can
 * republish them unchanged), original headers are kept, and error headers describe where and why it failed.
 */
public final class DeadLetters {
    static final int MAX_MESSAGE_CHARS = 1000;

    private static final Set<String> ERROR_HEADERS = Set.of(
            EventHeaders.ERROR_CLASS, EventHeaders.ERROR_MESSAGE, EventHeaders.SOURCE_TOPIC,
            EventHeaders.SOURCE_PARTITION, EventHeaders.SOURCE_OFFSET, EventHeaders.FAILED_AT,
            EventHeaders.ATTEMPTS, EventHeaders.NOT_BEFORE);

    private DeadLetters() {
    }

    /** Where the failed record came from. */
    public record Source(String topic, int partition, long offset) {
    }

    public static ProducerRecord<byte[], byte[]> record(Source source, byte[] key, byte[] value,
            Headers originalHeaders, Throwable error, int attempts, Instant failedAt) {
        ProducerRecord<byte[], byte[]> dlq = new ProducerRecord<>(Topics.DLQ, null, key, value);
        if (originalHeaders != null) {
            for (Header h : originalHeaders) {
                // A record can fail more than once (retry -> DLQ -> replay -> DLQ); keep only the latest error.
                if (!ERROR_HEADERS.contains(h.key())) {
                    dlq.headers().add(h.key(), h.value());
                }
            }
        }
        put(dlq, EventHeaders.ERROR_CLASS, error.getClass().getName());
        put(dlq, EventHeaders.ERROR_MESSAGE, truncate(String.valueOf(error.getMessage())));
        put(dlq, EventHeaders.SOURCE_TOPIC, source.topic());
        put(dlq, EventHeaders.SOURCE_PARTITION, Integer.toString(source.partition()));
        put(dlq, EventHeaders.SOURCE_OFFSET, Long.toString(source.offset()));
        put(dlq, EventHeaders.FAILED_AT, failedAt.toString());
        put(dlq, EventHeaders.ATTEMPTS, Integer.toString(attempts));
        return dlq;
    }

    /** Last value of a header as UTF-8 text, or null if absent. */
    public static String header(Headers headers, String name) {
        if (headers == null) {
            return null;
        }
        Header h = headers.lastHeader(name);
        return h == null || h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static void put(ProducerRecord<byte[], byte[]> r, String name, String value) {
        r.headers().remove(name);
        r.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String truncate(String s) {
        return s.length() <= MAX_MESSAGE_CHARS ? s : s.substring(0, MAX_MESSAGE_CHARS);
    }
}
