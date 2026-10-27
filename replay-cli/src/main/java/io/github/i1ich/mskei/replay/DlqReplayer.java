package io.github.i1ich.mskei.replay;

import io.github.i1ich.mskei.common.DeadLetters;
import io.github.i1ich.mskei.common.EventHeaders;
import io.github.i1ich.mskei.common.Topics;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reads the DLQ once, from the beginning, and republishes matching records to their source topic. */
public final class DlqReplayer {
    private static final Logger log = LoggerFactory.getLogger(DlqReplayer.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    /** Headers added by the DLQ writer; they must not travel back to the source topic. */
    private static final Set<String> STRIPPED_HEADERS = Set.of(
            EventHeaders.ERROR_CLASS, EventHeaders.ERROR_MESSAGE, EventHeaders.SOURCE_TOPIC,
            EventHeaders.SOURCE_PARTITION, EventHeaders.SOURCE_OFFSET, EventHeaders.FAILED_AT,
            EventHeaders.ATTEMPTS, EventHeaders.NOT_BEFORE);

    /**
     * Result counters. {@code replayed} is records sent (always 0 in dry-run);
     * {@code skipped} is records without a source-topic header.
     */
    public record Summary(long scanned, long matched, long replayed, long skipped, long failed) {
    }

    private final Consumer<byte[], byte[]> consumer;
    private final Producer<byte[], byte[]> producer;
    private final ReplayFilter filter;
    private final RateLimiter limiter;
    private final Long max;
    private final boolean dryRun;
    private final PrintStream out;

    public DlqReplayer(Consumer<byte[], byte[]> consumer, Producer<byte[], byte[]> producer, ReplayFilter filter,
            RateLimiter limiter, Long max, boolean dryRun, PrintStream out) {
        this.consumer = consumer;
        this.producer = producer;
        this.filter = filter;
        this.limiter = limiter;
        this.max = max;
        this.dryRun = dryRun;
        this.out = out;
    }

    public Summary run() throws InterruptedException {
        // assign() instead of subscribe(): no consumer group, so no rebalances and nothing is ever committed.
        // Replay is an operator action; it must not create or move the offsets of any group.
        List<TopicPartition> partitions = consumer.partitionsFor(Topics.DLQ).stream()
                .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        // Snapshot of the end: records that arrive in the DLQ while we replay are NOT read,
        // otherwise a record that fails again would be replayed in an endless loop.
        Map<TopicPartition, Long> endOffsets = new HashMap<>(consumer.endOffsets(partitions));

        long scanned = 0;
        long matched = 0;
        long replayed = 0;
        long skipped = 0;
        AtomicLong failed = new AtomicLong();
        boolean maxReached = false;

        while (!maxReached && !allPartitionsAtEnd(partitions, endOffsets)) {
            ConsumerRecords<byte[], byte[]> batch = consumer.poll(POLL_TIMEOUT);
            for (ConsumerRecord<byte[], byte[]> record : batch) {
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                if (record.offset() >= endOffsets.get(tp)) {
                    continue; // arrived after we started
                }
                scanned++;
                ReplayFilter.Verdict verdict = filter.evaluate(record.headers());
                if (verdict == ReplayFilter.Verdict.NO_SOURCE_TOPIC) {
                    skipped++;
                    log.warn("Skipping {}-{}@{}: no source-topic header", record.topic(), record.partition(),
                            record.offset());
                    continue;
                }
                if (verdict == ReplayFilter.Verdict.NO_MATCH) {
                    continue;
                }
                matched++;
                if (dryRun) {
                    printDryRun(record);
                } else {
                    limiter.acquire();
                    send(record, failed);
                    replayed++;
                }
                if (max != null && matched >= max) {
                    maxReached = true;
                    break;
                }
            }
        }
        // Wait until the broker acknowledged everything before we report numbers.
        producer.flush();
        return new Summary(scanned, matched, replayed, skipped, failed.get());
    }

    private boolean allPartitionsAtEnd(List<TopicPartition> partitions, Map<TopicPartition, Long> endOffsets) {
        for (TopicPartition tp : partitions) {
            if (consumer.position(tp) < endOffsets.get(tp)) {
                return false;
            }
        }
        return true;
    }

    private void send(ConsumerRecord<byte[], byte[]> dlq, AtomicLong failed) {
        String targetTopic = DeadLetters.header(dlq.headers(), EventHeaders.SOURCE_TOPIC);
        // Same key and value bytes; partition left to the producer, which hashes the key like the original write.
        ProducerRecord<byte[], byte[]> replay = new ProducerRecord<>(targetTopic, null, dlq.key(), dlq.value());
        for (Header h : dlq.headers()) {
            if (!STRIPPED_HEADERS.contains(h.key())) {
                replay.headers().add(h.key(), h.value());
            }
        }
        replay.headers().add(EventHeaders.REPLAYED_AT, Instant.now().toString().getBytes(StandardCharsets.UTF_8));
        producer.send(replay, (metadata, error) -> {
            if (error != null) {
                failed.incrementAndGet();
                log.error("Failed to replay DLQ offset {} to {}: {}", dlq.offset(), targetTopic, error.toString());
            }
        });
    }

    private void printDryRun(ConsumerRecord<byte[], byte[]> r) {
        out.printf("would replay: partition=%d offset=%d source-topic=%s error-class=%s event-id=%s key=%s%n",
                r.partition(), r.offset(),
                DeadLetters.header(r.headers(), EventHeaders.SOURCE_TOPIC),
                DeadLetters.header(r.headers(), EventHeaders.ERROR_CLASS),
                DeadLetters.header(r.headers(), EventHeaders.EVENT_ID),
                r.key() == null ? null : new String(r.key(), StandardCharsets.UTF_8));
    }
}
