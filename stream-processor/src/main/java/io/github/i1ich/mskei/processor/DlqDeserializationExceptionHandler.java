package io.github.i1ich.mskei.processor;

import io.github.i1ich.mskei.common.DeadLetters;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Safety net for records Kafka Streams itself cannot deserialize (before they reach our processors).
 * Both of our source topics are read as raw bytes, so this should never fire today; it protects us if somebody
 * later switches a source to a typed serde.
 *
 * <p>It writes the raw record to the DLQ with its OWN small producer and then CONTINUEs. That producer is NOT part
 * of the exactly-once transaction (the handler runs outside the processing transaction), so on a crash a record
 * could reach the DLQ twice. DLQ consumers must tolerate duplicates; the alternative (losing data) is worse.
 */
public class DlqDeserializationExceptionHandler implements DeserializationExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DlqDeserializationExceptionHandler.class);

    private final Supplier<Producer<byte[], byte[]>> producerFactory;
    private Map<String, Object> streamsConfigs = Map.of();
    private Producer<byte[], byte[]> producer;

    /** Used by Kafka Streams (instantiated by class name). */
    public DlqDeserializationExceptionHandler() {
        this.producerFactory = null;
    }

    /** For tests: inject a MockProducer. */
    DlqDeserializationExceptionHandler(Supplier<Producer<byte[], byte[]>> producerFactory) {
        this.producerFactory = producerFactory;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.streamsConfigs = new HashMap<>(configs);
    }

    @Override
    public synchronized DeserializationHandlerResponse handle(ErrorHandlerContext context,
            ConsumerRecord<byte[], byte[]> record, Exception exception) {
        try {
            var source = new DeadLetters.Source(record.topic(), record.partition(), record.offset());
            var dlq = DeadLetters.record(source, record.key(), record.value(), record.headers(), exception, 1,
                    Instant.now());
            // Wait for the ack: if the DLQ write fails we must NOT silently skip the record.
            producer().send(dlq).get();
            LOG.warn("Deserialization failure, sent {}-{}-{} to DLQ: {}", source.topic(), source.partition(),
                    source.offset(), exception.toString());
            return DeserializationHandlerResponse.CONTINUE;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return DeserializationHandlerResponse.FAIL;
        } catch (ExecutionException | RuntimeException e) {
            LOG.error("Could not write to DLQ, failing the stream thread", e);
            return DeserializationHandlerResponse.FAIL;
        }
    }

    private Producer<byte[], byte[]> producer() {
        if (producer == null) {
            producer = producerFactory != null ? producerFactory.get() : newProducer();
        }
        return producer;
    }

    /** Idempotent (no duplicates from producer retries) but NOT transactional. Reuses the app's connection settings. */
    private Producer<byte[], byte[]> newProducer() {
        Map<String, Object> cfg = new HashMap<>();
        streamsConfigs.forEach((k, v) -> {
            if (k.equals("bootstrap.servers") || k.startsWith("security.") || k.startsWith("sasl.")
                    || k.startsWith("ssl.")) {
                cfg.put(k, v);
            }
        });
        cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        cfg.put(ProducerConfig.ACKS_CONFIG, "all");
        cfg.put(ProducerConfig.CLIENT_ID_CONFIG, "stream-processor-dlq-handler");
        return new KafkaProducer<>(cfg, new ByteArraySerializer(), new ByteArraySerializer());
    }
}
