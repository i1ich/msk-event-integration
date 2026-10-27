package io.github.i1ich.mskei.processor;

import io.github.i1ich.mskei.common.KafkaClientConfig;
import io.github.i1ich.mskei.common.SchemaRegistryConfig;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams application (topologies A and B, see {@link ProcessorTopology}).
 *
 * <h2>What exactly_once_v2 covers</h2>
 * Consume, process and produce form ONE Kafka transaction: input offsets are committed together with the
 * output records, and so are the DLQ records written by the topology (they are just another output topic).
 * After a crash the work is either fully visible or fully rolled back; consumers must read with
 * {@code isolation.level=read_committed} to not see rolled-back records.
 *
 * <h2>What it does NOT cover</h2>
 * <ul>
 *   <li>{@link DlqDeserializationExceptionHandler}: it uses its own non-transactional producer.</li>
 *   <li>Anything outside Kafka, e.g. the HTTP calls made by delivery-service: they are at-least-once, which is
 *       why every event carries a deterministic eventId used as Idempotency-Key.</li>
 * </ul>
 *
 * <p>Environment: KAFKA_BOOTSTRAP_SERVERS, KAFKA_SECURITY, SCHEMA_REGISTRY_URL, SCHEMA_AUTO_REGISTER (see
 * {@code common}), plus STREAMS_APPLICATION_ID (default "stream-processor") and STREAMS_REPLICATION_FACTOR
 * (optional; set 3 on a multi-broker cluster).
 */
public final class StreamProcessorApp {
    private static final Logger LOG = LoggerFactory.getLogger(StreamProcessorApp.class);

    static final String ENV_APPLICATION_ID = "STREAMS_APPLICATION_ID";
    static final String ENV_REPLICATION_FACTOR = "STREAMS_REPLICATION_FACTOR";
    static final String DEFAULT_APPLICATION_ID = "stream-processor";

    private StreamProcessorApp() {
    }

    public static void main(String[] args) {
        KafkaStreams streams = start(System.getenv());
        // close() lets Streams finish the current transaction and leave the consumer group cleanly.
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close, "streams-shutdown"));
    }

    /**
     * Builds and starts the app without blocking and without a shutdown hook (the caller owns the lifecycle:
     * {@code main} adds a hook, integration tests call {@code close()}).
     */
    public static KafkaStreams start(Map<String, String> env) {
        KafkaStreams streams = new KafkaStreams(
                ProcessorTopology.build(SchemaRegistryConfig.from(env)), streamsProperties(env));

        streams.setStateListener((newState, oldState) -> LOG.info("State change: {} -> {}", oldState, newState));
        // A bug or a broker hiccup in one thread should not take the whole app down: start a fresh thread.
        streams.setUncaughtExceptionHandler(e -> {
            LOG.error("Stream thread died, replacing it", e);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        streams.start();
        LOG.info("stream-processor started");
        return streams;
    }

    static Properties streamsProperties(Map<String, String> env) {
        Properties props = KafkaClientConfig.from(env);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, env.getOrDefault(ENV_APPLICATION_ID, DEFAULT_APPLICATION_ID));
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                DlqDeserializationExceptionHandler.class);
        String replication = env.get(ENV_REPLICATION_FACTOR);
        if (replication != null && !replication.isBlank()) {
            props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, replication);
        }
        return props;
    }
}
