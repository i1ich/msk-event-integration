package io.github.i1ich.mskei.delivery;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.github.i1ich.mskei.common.DeadLetters;
import io.github.i1ich.mskei.common.EventHeaders;
import io.github.i1ich.mskei.common.Topics;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Future;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles ONE record: deliver it, or write it to the next retry topic / the DLQ.
 * Knows nothing about polling or offsets, so it can be tested with a MockProducer and WireMock.
 */
public final class RecordHandler {
    private static final Logger log = LoggerFactory.getLogger(RecordHandler.class);

    private final KafkaAvroDeserializer deserializer;
    private final DownstreamSender sender;
    private final Producer<byte[], byte[]> producer;
    private final RetryRouter router;

    public RecordHandler(KafkaAvroDeserializer deserializer, DownstreamSender sender,
            Producer<byte[], byte[]> producer, RetryRouter router) {
        this.deserializer = deserializer;
        this.sender = sender;
        this.producer = producer;
        this.router = router;
    }

    /**
     * @return the pending write to Kafka if the record had to be retried or dead-lettered (the caller must wait
     *         for it before committing the offset), or empty if the record was delivered.
     */
    public Optional<Future<RecordMetadata>> handle(ConsumerRecord<byte[], byte[]> record) {
        // Type decision uses the ORIGINAL topic: on retry topics it is carried in the source-topic header.
        String originalTopic = RetryRouter.isRetryTopic(record.topic())
                ? DeadLetters.header(record.headers(), EventHeaders.SOURCE_TOPIC)
                : record.topic();

        String path;
        String eventId;
        String json;
        try {
            path = pathFor(originalTopic);
            GenericRecord event = (GenericRecord) deserializer.deserialize(originalTopic, record.value());
            eventId = event.get("eventId").toString();
            // GenericRecord.toString() is JSON for our schemas (strings quoted/escaped, enums as strings,
            // timestamps as numbers, nulls as null). Simpler than wiring a JsonEncoder.
            json = event.toString();
        } catch (RuntimeException e) {
            // Poison pill (garbage bytes, unknown schema, missing eventId, unknown topic): retrying is pointless.
            log.warn("Cannot decode record {}-{}@{}, sending to DLQ", record.topic(), record.partition(),
                    record.offset(), e);
            return Optional.of(producer.send(router.onPermanent(record, String.valueOf(originalTopic), e)));
        }

        Throwable failure;
        try {
            int status = sender.post(path, eventId, json);
            if (Classifier.classify(status) == Outcome.SUCCESS) {
                log.info("Delivered eventId={} to {} (attempt {})", eventId, path,
                        RetryRouter.attemptOf(record.topic()));
                return Optional.empty();
            }
            failure = new DownstreamException(status);
        } catch (IOException e) {
            failure = e;
        }

        ProducerRecord<byte[], byte[]> next = Classifier.classify(failure) == Outcome.RETRYABLE
                ? router.onRetryable(record, originalTopic, failure)
                : router.onPermanent(record, originalTopic, failure);
        log.warn("Delivery of eventId={} failed ({}), writing to {}", eventId, failure.getMessage(), next.topic());
        return Optional.of(producer.send(next));
    }

    private static String pathFor(String originalTopic) {
        if (Topics.ACCOUNT_V1.equals(originalTopic)) {
            return "/accounts";
        }
        if (Topics.ORDER_V1.equals(originalTopic)) {
            return "/orders";
        }
        throw new IllegalArgumentException("No downstream route for topic " + originalTopic);
    }
}
