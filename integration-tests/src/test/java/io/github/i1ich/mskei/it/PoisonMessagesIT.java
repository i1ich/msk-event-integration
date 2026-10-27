package io.github.i1ich.mskei.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.i1ich.mskei.common.EventHeaders;
import io.github.i1ich.mskei.common.Topics;
import io.github.i1ich.mskei.it.support.Events;
import io.github.i1ich.mskei.it.support.TopicTail;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

/** Records the processor cannot map end up in the DLQ with the original bytes; the processor keeps running. */
class PoisonMessagesIT extends AbstractIT {

    @Test
    void unknownEntityGoesToDlqWithOriginalBytesAndErrorHeaders() {
        String id = "006" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase() + "AAA";
        String body = Events.salesforce("Opportunity", "CREATE", id, Map.of("Name", "Big Deal", "Amount", 50000));

        assertThat(ENV.ingress().handle(body, Map.of()).status()).isEqualTo(202);

        try (TopicTail dlq = ENV.tail(Topics.DLQ)) {
            ConsumerRecord<byte[], byte[]> dead = dlq.awaitOne(TopicTail.keyIs(id));
            assertThat(TopicTail.header(dead, EventHeaders.ERROR_CLASS)).endsWith(".UnmappedEntityException");
            assertThat(TopicTail.header(dead, EventHeaders.SOURCE_TOPIC)).isEqualTo(Topics.SF_CHANGE_EVENTS_RAW);
            assertThat(TopicTail.header(dead, EventHeaders.ATTEMPTS)).isEqualTo("1");
            assertThat(TopicTail.header(dead, EventHeaders.ERROR_MESSAGE)).contains("Opportunity");
            assertThat(TopicTail.header(dead, EventHeaders.FAILED_AT)).isNotBlank();
            // The original ingress headers survive, so the record can be traced and replayed.
            assertThat(TopicTail.header(dead, EventHeaders.EVENT_ID)).isNotBlank();
            assertThat(new String(dead.value(), StandardCharsets.UTF_8)).isEqualTo(body);
        }
    }

    @Test
    void garbageBytesOnOrdersTopicGoToDlqAndProcessingContinues() throws Exception {
        String key = "garbage-" + UUID.randomUUID();
        byte[] garbage = new byte[] {0x00, 0x00, 0x00, 0x7f, (byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef, 0x01};
        try (Producer<byte[], byte[]> producer = ENV.bytesProducer()) {
            producer.send(new ProducerRecord<>(Topics.ERP_ORDERS_CDC, key.getBytes(StandardCharsets.UTF_8), garbage))
                    .get();
        }

        try (TopicTail dlq = ENV.tail(Topics.DLQ); TopicTail accounts = ENV.tail(Topics.ACCOUNT_V1)) {
            ConsumerRecord<byte[], byte[]> dead = dlq.awaitOne(TopicTail.keyIs(key));
            assertThat(TopicTail.header(dead, EventHeaders.SOURCE_TOPIC)).isEqualTo(Topics.ERP_ORDERS_CDC);
            assertThat(TopicTail.header(dead, EventHeaders.ERROR_CLASS)).isNotBlank();
            assertThat(dead.value()).isEqualTo(garbage);

            // The stream threads survived the poison pill: a valid event after it is still processed.
            String accountId = Events.accountId();
            assertThat(ENV.ingress().handle(Events.account("CREATE", accountId, Map.of("Name", "After poison")),
                    Map.of()).status()).isEqualTo(202);
            assertThat(accounts.awaitOne(TopicTail.keyIs(accountId))).isNotNull();
        }
    }
}
