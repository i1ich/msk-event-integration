package io.github.i1ich.mskei.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.i1ich.mskei.common.EventHeaders;
import io.github.i1ich.mskei.common.Topics;
import io.github.i1ich.mskei.it.support.Downstream;
import io.github.i1ich.mskei.it.support.Events;
import io.github.i1ich.mskei.it.support.TopicTail;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/** Retry ladder and DLQ of delivery-service, driven by the WireMock magic ids (503 once / always 400). */
class DeliveryRetryIT extends AbstractIT {
    private static final String FAIL_ONCE = "001xx000000FAIL1AAA";
    private static final String REJECTED = "001xx000000REJECTAAA";

    @Test
    void transientDownstreamFailureIsRetriedViaRetryTopicWithSameIdempotencyKey() {
        assertThat(ENV.ingress().handle(Events.account("UPDATE", FAIL_ONCE, Map.of("Phone", "+1-555-0142")),
                Map.of()).status()).isEqualTo(202);

        List<Downstream.Call> calls = Downstream.awaitCalls(ENV.wireMock(), "/downstream/accounts",
                b -> FAIL_ONCE.equals(b.path("accountId").asText()), 2);

        assertThat(calls).hasSize(2); // 503, then 200
        assertThat(calls.get(0).idempotencyKey()).isNotBlank().isEqualTo(calls.get(1).idempotencyKey());

        try (TopicTail retry = ENV.tail(Topics.RETRY_1M); TopicTail dlq = ENV.tail(Topics.DLQ)) {
            ConsumerRecord<byte[], byte[]> retried = retry.awaitOne(TopicTail.keyIs(FAIL_ONCE));
            assertThat(TopicTail.header(retried, EventHeaders.ATTEMPTS)).isEqualTo("1");
            assertThat(TopicTail.header(retried, EventHeaders.SOURCE_TOPIC)).isEqualTo(Topics.ACCOUNT_V1);
            assertThat(Instant.parse(TopicTail.header(retried, EventHeaders.NOT_BEFORE))).isNotNull();
            // Recovered after one retry: it must not have reached the DLQ.
            dlq.assertNone(TopicTail.keyIs(FAIL_ONCE), Duration.ofSeconds(4));
        }
        // And it was not delivered a third time.
        assertThat(Downstream.calls(ENV.wireMock(), "/downstream/accounts",
                b -> FAIL_ONCE.equals(b.path("accountId").asText()))).hasSize(2);
    }

    @Test
    void permanentDownstreamRejectionGoesStraightToDlqWithoutRetry() {
        assertThat(ENV.ingress().handle(Events.account("UPDATE", REJECTED, Map.of("Name", "Initech")), Map.of())
                .status()).isEqualTo(202);

        try (TopicTail dlq = ENV.tail(Topics.DLQ);
                TopicTail retry1 = ENV.tail(Topics.RETRY_1M);
                TopicTail retry10 = ENV.tail(Topics.RETRY_10M)) {
            ConsumerRecord<byte[], byte[]> dead = dlq.awaitOne(TopicTail.keyIs(REJECTED));
            assertThat(TopicTail.header(dead, EventHeaders.ERROR_CLASS)).endsWith(".DownstreamException");
            assertThat(TopicTail.header(dead, EventHeaders.SOURCE_TOPIC)).isEqualTo(Topics.ACCOUNT_V1);
            assertThat(TopicTail.header(dead, EventHeaders.ATTEMPTS)).isEqualTo("1");

            retry1.assertNone(TopicTail.keyIs(REJECTED), Duration.ofSeconds(3));
            retry10.assertNone(TopicTail.keyIs(REJECTED), Duration.ofSeconds(1));
        }
        // A 400 is not retried: exactly one call.
        assertThat(Downstream.calls(ENV.wireMock(), "/downstream/accounts",
                b -> REJECTED.equals(b.path("accountId").asText()))).hasSize(1);
    }
}
