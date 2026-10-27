package io.github.i1ich.mskei.it.support;

import io.github.i1ich.mskei.common.DeadLetters;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;

/**
 * Reads one topic from the beginning with its own consumer (unique group id, no commits) and keeps every
 * record it has seen. Asynchronous assertions go through Awaitility, never fixed sleeps.
 *
 * <p>Reads with read_committed, like any real consumer of the exactly_once_v2 output topics.
 */
public final class TopicTail implements AutoCloseable {
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final List<ConsumerRecord<byte[], byte[]>> seen = new ArrayList<>();

    TopicTail(String bootstrapServers, String topic) {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrapServers);
        p.put("group.id", "it-" + UUID.randomUUID());
        p.put("enable.auto.commit", "false");
        p.put("auto.offset.reset", "earliest");
        p.put("isolation.level", "read_committed");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumer = new KafkaConsumer<>(p);
        List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                .map(i -> new TopicPartition(i.topic(), i.partition())).toList();
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
    }

    /** Polls until at least {@code count} records match; returns the matching ones (in arrival order). */
    public List<ConsumerRecord<byte[], byte[]>> await(Predicate<ConsumerRecord<byte[], byte[]>> match, int count) {
        Awaitility.await().until(() -> {
            fetch();
            return matching(match).size() >= count;
        });
        return matching(match);
    }

    public ConsumerRecord<byte[], byte[]> awaitOne(Predicate<ConsumerRecord<byte[], byte[]>> match) {
        return await(match, 1).get(0);
    }

    /** Asserts that no matching record appears for {@code period} (a "nothing happens" check, bounded wait). */
    public void assertNone(Predicate<ConsumerRecord<byte[], byte[]>> match, Duration period) {
        Awaitility.await().during(period).atMost(period.plusSeconds(30)).until(() -> {
            fetch();
            return matching(match).isEmpty();
        });
    }

    /** Waits for exactly {@code count} matching records and checks the number stays so for {@code period}. */
    public List<ConsumerRecord<byte[], byte[]>> awaitStableCount(Predicate<ConsumerRecord<byte[], byte[]>> match,
            int count, Duration period) {
        await(match, count);
        Awaitility.await().during(period).atMost(period.plusSeconds(30)).until(() -> {
            fetch();
            return matching(match).size() == count;
        });
        return matching(match);
    }

    private void fetch() {
        consumer.poll(Duration.ofMillis(200)).forEach(seen::add);
    }

    private List<ConsumerRecord<byte[], byte[]>> matching(Predicate<ConsumerRecord<byte[], byte[]>> match) {
        return seen.stream().filter(match).toList();
    }

    @Override
    public void close() {
        consumer.close(Duration.ofSeconds(2));
    }

    // ---- predicates and header helpers -----------------------------------------------------------------------

    public static Predicate<ConsumerRecord<byte[], byte[]>> keyIs(String key) {
        return r -> r.key() != null && key.equals(new String(r.key(), StandardCharsets.UTF_8));
    }

    public static String header(ConsumerRecord<byte[], byte[]> r, String name) {
        return DeadLetters.header(r.headers(), name);
    }

    public static String keyText(ConsumerRecord<byte[], byte[]> r) {
        return r.key() == null ? null : new String(r.key(), StandardCharsets.UTF_8);
    }
}
