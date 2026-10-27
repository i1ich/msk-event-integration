package io.github.i1ich.mskei.delivery;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * poll -> handle every record -> wait for retry/DLQ writes -> commit. One instance per consumer thread.
 *
 * <p>With {@code honourNotBefore} (retry consumer) a record whose {@code not-before} is in the future blocks
 * its partition. That is correct, not a bug: all records in one retry topic have the same delay, so they are
 * in due-time order and nothing behind the head can be due earlier.
 */
public final class ConsumerLoop implements Runnable, ConsumerRebalanceListener {
    private static final Logger log = LoggerFactory.getLogger(ConsumerLoop.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final String name;
    private final Consumer<byte[], byte[]> consumer;
    private final List<String> topics;
    private final RecordHandler handler;
    private final boolean honourNotBefore;
    private final Clock clock;
    /** Paused partitions and the moment each one may resume. Only touched by the consumer thread. */
    private final Map<TopicPartition, Instant> pausedUntil = new HashMap<>();

    public ConsumerLoop(String name, Consumer<byte[], byte[]> consumer, List<String> topics,
            RecordHandler handler, boolean honourNotBefore, Clock clock) {
        this.name = name;
        this.consumer = consumer;
        this.topics = topics;
        this.handler = handler;
        this.honourNotBefore = honourNotBefore;
        this.clock = clock;
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics, this);
            log.info("{} consumer started on {}", name, topics);
            while (true) {
                pollOnce();
            }
        } catch (WakeupException e) {
            log.info("{} consumer stopping", name);
        } finally {
            consumer.close();
        }
    }

    /** Thread-safe: makes a blocked poll() throw WakeupException so run() ends. */
    public void stop() {
        consumer.wakeup();
    }

    void pollOnce() {
        resumeDuePartitions();
        // Keep calling poll() even when every partition is paused: that is what keeps us in the group
        // (otherwise max.poll.interval.ms would expire during a 10 minute wait and trigger a rebalance).
        ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_TIMEOUT);

        List<Future<RecordMetadata>> pendingWrites = new ArrayList<>();
        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        for (TopicPartition tp : records.partitions()) {
            for (ConsumerRecord<byte[], byte[]> record : records.records(tp)) {
                if (honourNotBefore) {
                    Instant notBefore = NotBefore.parse(record.headers());
                    if (notBefore != null && notBefore.isAfter(clock.instant())) {
                        // Not yet due: rewind so this record is fetched again, pause the partition, move on.
                        consumer.seek(tp, record.offset());
                        consumer.pause(List.of(tp));
                        pausedUntil.put(tp, notBefore);
                        break;
                    }
                }
                handler.handle(record).ifPresent(pendingWrites::add);
                toCommit.put(tp, new OffsetAndMetadata(record.offset() + 1));
            }
        }

        // Commit only when every retry/DLQ write is acknowledged by the broker. A crash before this line means
        // the batch is read again: duplicate POSTs are possible and are absorbed by the Idempotency-Key.
        awaitAll(pendingWrites);
        if (!toCommit.isEmpty()) {
            consumer.commitSync(toCommit);
        }
    }

    private void resumeDuePartitions() {
        Instant now = clock.instant();
        pausedUntil.entrySet().removeIf(entry -> {
            if (entry.getValue().isAfter(now)) {
                return false;
            }
            consumer.resume(List.of(entry.getKey()));
            return true;
        });
    }

    private static void awaitAll(List<Future<RecordMetadata>> writes) {
        for (Future<RecordMetadata> write : writes) {
            try {
                write.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for Kafka write", e);
            } catch (ExecutionException e) {
                // Not committing is the safe answer: crash, restart, and process the batch again.
                throw new IllegalStateException("Retry/DLQ write failed", e.getCause());
            }
        }
    }

    // After a rebalance the new owner reads the head record again and re-pauses if it is still not due,
    // so there is nothing to carry over; just forget partitions we no longer own.
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        partitions.forEach(pausedUntil::remove);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // Nothing to do: see the comment above.
    }
}
