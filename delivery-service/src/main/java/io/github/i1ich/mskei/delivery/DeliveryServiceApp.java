package io.github.i1ich.mskei.delivery;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.github.i1ich.mskei.common.KafkaClientConfig;
import io.github.i1ich.mskei.common.SchemaRegistryConfig;
import io.github.i1ich.mskei.common.Topics;
import java.time.Clock;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delivers canonical events to downstream HTTP. Two consumers in one process (two threads):
 * the main one (account + order topics) and the retry one (retry.1m + retry.10m).
 */
public final class DeliveryServiceApp {
    private static final Logger log = LoggerFactory.getLogger(DeliveryServiceApp.class);

    private DeliveryServiceApp() {
    }

    public static void main(String[] args) throws InterruptedException {
        DeliveryConfig config = DeliveryConfig.fromEnv();
        Clock clock = Clock.systemUTC();

        // Idempotent producer + acks=all: a retry/DLQ write is never lost or duplicated by producer retries.
        Properties producerProps = KafkaClientConfig.fromEnv();
        producerProps.put("enable.idempotence", "true");
        producerProps.put("acks", "all");
        KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer());

        KafkaAvroDeserializer avro = new KafkaAvroDeserializer();
        avro.configure(SchemaRegistryConfig.fromEnv(), false);

        RetryRouter router = new RetryRouter(clock, config.retryDelay1m(), config.retryDelay10m());
        RecordHandler handler = new RecordHandler(avro,
                new HttpDownstreamSender(config.downstreamUrl(), config.connectTimeout(), config.requestTimeout()),
                producer, router);

        ConsumerLoop main = new ConsumerLoop("main", newConsumer("delivery-service"),
                List.of(Topics.ACCOUNT_V1, Topics.ORDER_V1), handler, false, clock);
        ConsumerLoop retry = new ConsumerLoop("retry", newConsumer("delivery-service-retry"),
                List.of(Topics.RETRY_1M, Topics.RETRY_10M), handler, true, clock);

        Thread mainThread = thread("main-consumer", main);
        Thread retryThread = thread("retry-consumer", retry);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown requested");
            main.stop();
            retry.stop();
            join(mainThread);
            join(retryThread);
            producer.close();
        }));

        mainThread.start();
        retryThread.start();
        mainThread.join();
        retryThread.join();
        producer.close();
    }

    private static KafkaConsumer<byte[], byte[]> newConsumer(String groupId) {
        Properties props = KafkaClientConfig.fromEnv();
        props.put("group.id", groupId);
        // We commit by hand, after the work is done (see ConsumerLoop).
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        // Small batches: one slow downstream must not make a poll cycle take minutes.
        props.put("max.poll.records", "50");
        return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    private static Thread thread(String name, ConsumerLoop loop) {
        Thread t = new Thread(loop, name);
        // If a loop dies (e.g. a Kafka write failed) the whole process exits and the orchestrator restarts it;
        // uncommitted records are then processed again.
        t.setUncaughtExceptionHandler((th, e) -> {
            log.error("{} crashed, exiting", th.getName(), e);
            Runtime.getRuntime().halt(1);
        });
        return t;
    }

    private static void join(Thread t) {
        try {
            t.join(10_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
