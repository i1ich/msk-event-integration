package io.github.i1ich.mskei.delivery;

import static io.github.i1ich.mskei.delivery.TestEvents.header;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.i1ich.mskei.common.Topics;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

class RetryRouterTest {
    private static final Instant NOW = Instant.parse("2026-01-01T12:00:00Z");
    private final RetryRouter router = new RetryRouter(Clock.fixed(NOW, ZoneOffset.UTC),
            Duration.ofMinutes(1), Duration.ofMinutes(10));
    private final Throwable error = new DownstreamException(503);

    @Test
    void attemptNumbersFollowTheTopic() {
        assertThat(RetryRouter.attemptOf(Topics.ACCOUNT_V1)).isEqualTo(1);
        assertThat(RetryRouter.attemptOf(Topics.RETRY_1M)).isEqualTo(2);
        assertThat(RetryRouter.attemptOf(Topics.RETRY_10M)).isEqualTo(3);
    }

    @Test
    void firstFailureGoesToRetry1m() {
        ConsumerRecord<byte[], byte[]> failed = TestEvents.record(Topics.ACCOUNT_V1, new byte[] {1, 2});
        failed.headers().add("event-id", "e1".getBytes(StandardCharsets.UTF_8));

        ProducerRecord<byte[], byte[]> out = router.onRetryable(failed, Topics.ACCOUNT_V1, error);

        assertThat(out.topic()).isEqualTo(Topics.RETRY_1M);
        assertThat(out.key()).isEqualTo(failed.key());
        assertThat(out.value()).isEqualTo(new byte[] {1, 2});
        assertThat(header(out, "source-topic")).isEqualTo(Topics.ACCOUNT_V1);
        assertThat(header(out, "attempts")).isEqualTo("1");
        assertThat(header(out, "not-before")).isEqualTo("2026-01-01T12:01:00Z");
        // original headers are kept
        assertThat(header(out, "event-id")).isEqualTo("e1");
    }

    @Test
    void secondFailureGoesToRetry10mAndReplacesOurHeaders() {
        ConsumerRecord<byte[], byte[]> failed =
                TestEvents.retryRecord(Topics.RETRY_1M, Topics.ORDER_V1, new byte[] {9});
        failed.headers().add("attempts", "1".getBytes(StandardCharsets.UTF_8));
        failed.headers().add("not-before", "2020-01-01T00:00:00Z".getBytes(StandardCharsets.UTF_8));

        ProducerRecord<byte[], byte[]> out = router.onRetryable(failed, Topics.ORDER_V1, error);

        assertThat(out.topic()).isEqualTo(Topics.RETRY_10M);
        assertThat(header(out, "source-topic")).isEqualTo(Topics.ORDER_V1);
        assertThat(header(out, "attempts")).isEqualTo("2");
        assertThat(header(out, "not-before")).isEqualTo("2026-01-01T12:10:00Z");
        assertThat(out.headers().headers("not-before")).hasSize(1);
        assertThat(out.headers().headers("attempts")).hasSize(1);
    }

    @Test
    void thirdFailureGoesToDlq() {
        ConsumerRecord<byte[], byte[]> failed =
                TestEvents.retryRecord(Topics.RETRY_10M, Topics.ACCOUNT_V1, new byte[] {9});

        ProducerRecord<byte[], byte[]> out = router.onRetryable(failed, Topics.ACCOUNT_V1, error);

        assertThat(out.topic()).isEqualTo(Topics.DLQ);
        assertThat(header(out, "attempts")).isEqualTo("3");
        assertThat(header(out, "source-topic")).isEqualTo(Topics.ACCOUNT_V1);
        assertThat(header(out, "source-offset")).isEqualTo("3");
        assertThat(out.value()).isEqualTo(new byte[] {9});
    }

    @Test
    void permanentFailureGoesStraightToDlqWithOneAttempt() {
        ConsumerRecord<byte[], byte[]> failed = TestEvents.record(Topics.ACCOUNT_V1, new byte[] {5});

        ProducerRecord<byte[], byte[]> out =
                router.onPermanent(failed, Topics.ACCOUNT_V1, new DownstreamException(400));

        assertThat(out.topic()).isEqualTo(Topics.DLQ);
        assertThat(header(out, "attempts")).isEqualTo("1");
        assertThat(header(out, "error-class")).isEqualTo(DownstreamException.class.getName());
        assertThat(header(out, "error-message")).contains("400");
        assertThat(header(out, "source-partition")).isEqualTo("0");
        assertThat(header(out, "source-offset")).isEqualTo("7");
        assertThat(header(out, "failed-at")).isEqualTo(NOW.toString());
    }

    @Test
    void ioErrorsAreReportedInDlq() {
        ConsumerRecord<byte[], byte[]> failed =
                TestEvents.retryRecord(Topics.RETRY_10M, Topics.ACCOUNT_V1, new byte[0]);
        ProducerRecord<byte[], byte[]> out = router.onRetryable(failed, Topics.ACCOUNT_V1, new IOException("timeout"));
        assertThat(header(out, "error-class")).isEqualTo("java.io.IOException");
    }
}
