package io.github.i1ich.mskei.delivery;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static io.github.i1ich.mskei.delivery.TestEvents.header;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.github.i1ich.mskei.common.Topics;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RecordHandlerTest {
    private static WireMockServer wiremock;
    private final MockProducer<byte[], byte[]> producer =
            new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
    private final RetryRouter router = new RetryRouter(
            Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC),
            Duration.ofMinutes(1), Duration.ofMinutes(10));
    private RecordHandler handler;

    @BeforeAll
    static void startWireMock() {
        wiremock = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        wiremock.start();
    }

    @AfterAll
    static void stopWireMock() {
        wiremock.stop();
    }

    @BeforeEach
    void setUp() {
        wiremock.resetAll();
        handler = new RecordHandler(TestEvents.deserializer(),
                new HttpDownstreamSender("http://localhost:" + wiremock.port() + "/", Duration.ofSeconds(2),
                        Duration.ofSeconds(5)),
                producer, router);
    }

    @Test
    void successSendsNothingToKafkaAndPostsJsonWithIdempotencyKey() {
        wiremock.stubFor(post(urlEqualTo("/accounts")).willReturn(aResponse().withStatus(200)));

        Optional<Future<RecordMetadata>> result =
                handler.handle(TestEvents.record(Topics.ACCOUNT_V1, TestEvents.account("evt-1", "001A")));

        assertThat(result).isEmpty();
        assertThat(producer.history()).isEmpty();
        wiremock.verify(postRequestedFor(urlEqualTo("/accounts"))
                .withHeader("Idempotency-Key", equalTo("evt-1"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withRequestBody(equalToJson(
                        "{\"eventId\":\"evt-1\",\"accountId\":\"001A\",\"changeType\":\"UPDATE\","
                                + "\"name\":\"Acme \\\"quoted\\\"\",\"commitTimestamp\":1767225600000,"
                                + "\"sourceSystem\":\"salesforce\"}", true, true)));
    }

    @Test
    void ordersGoToOrdersPath() {
        wiremock.stubFor(post(urlEqualTo("/orders")).willReturn(aResponse().withStatus(201)));

        handler.handle(TestEvents.record(Topics.ORDER_V1, TestEvents.order("evt-2", "001A")));

        wiremock.verify(1, postRequestedFor(urlEqualTo("/orders")).withHeader("Idempotency-Key", equalTo("evt-2")));
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void pathIsDecidedByOriginalTopicOnRetryTopics() {
        wiremock.stubFor(post(urlEqualTo("/orders")).willReturn(aResponse().withStatus(200)));

        handler.handle(TestEvents.retryRecord(Topics.RETRY_1M, Topics.ORDER_V1, TestEvents.order("evt-3", "001A")));

        wiremock.verify(1, postRequestedFor(urlEqualTo("/orders")));
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void serverErrorGoesToRetry1m() {
        wiremock.stubFor(post(urlEqualTo("/accounts")).willReturn(aResponse().withStatus(503)));

        Optional<Future<RecordMetadata>> result =
                handler.handle(TestEvents.record(Topics.ACCOUNT_V1, TestEvents.account("evt-4", "001A")));

        assertThat(result).isPresent();
        assertThat(producer.history()).hasSize(1);
        assertThat(producer.history().get(0).topic()).isEqualTo(Topics.RETRY_1M);
        assertThat(header(producer.history().get(0), "source-topic")).isEqualTo(Topics.ACCOUNT_V1);
    }

    @Test
    void failureInRetry10mGoesToDlqWithThreeAttempts() {
        wiremock.stubFor(post(urlEqualTo("/accounts")).willReturn(aResponse().withStatus(500)));

        handler.handle(TestEvents.retryRecord(Topics.RETRY_10M, Topics.ACCOUNT_V1,
                TestEvents.account("evt-5", "001A")));

        assertThat(producer.history()).hasSize(1);
        assertThat(producer.history().get(0).topic()).isEqualTo(Topics.DLQ);
        assertThat(header(producer.history().get(0), "attempts")).isEqualTo("3");
    }

    @Test
    void clientErrorGoesStraightToDlq() {
        wiremock.stubFor(post(urlEqualTo("/accounts")).willReturn(aResponse().withStatus(400)));

        handler.handle(TestEvents.record(Topics.ACCOUNT_V1, TestEvents.account("evt-6", "001A")));

        assertThat(producer.history()).hasSize(1);
        assertThat(producer.history().get(0).topic()).isEqualTo(Topics.DLQ);
        assertThat(header(producer.history().get(0), "attempts")).isEqualTo("1");
        assertThat(header(producer.history().get(0), "error-message")).contains("400");
    }

    @Test
    void networkErrorIsRetried() {
        DownstreamSender broken = (path, key, json) -> {
            throw new IOException("connection refused");
        };
        RecordHandler h = new RecordHandler(TestEvents.deserializer(), broken, producer, router);

        h.handle(TestEvents.record(Topics.ACCOUNT_V1, TestEvents.account("evt-7", "001A")));

        assertThat(producer.history().get(0).topic()).isEqualTo(Topics.RETRY_1M);
    }

    @Test
    void slowDownstreamTimesOutAndIsRetried() {
        wiremock.stubFor(post(urlEqualTo("/accounts")).willReturn(aResponse().withStatus(200).withFixedDelay(1500)));
        RecordHandler h = new RecordHandler(TestEvents.deserializer(),
                new HttpDownstreamSender("http://localhost:" + wiremock.port(), Duration.ofSeconds(2),
                        Duration.ofMillis(200)),
                producer, router);

        h.handle(TestEvents.record(Topics.ACCOUNT_V1, TestEvents.account("evt-8", "001A")));

        assertThat(producer.history().get(0).topic()).isEqualTo(Topics.RETRY_1M);
    }

    @Test
    void garbageBytesGoToDlqWithoutCallingDownstream() {
        handler.handle(TestEvents.record(Topics.ACCOUNT_V1, new byte[] {0, 0, 0, 0, 99, 1, 2, 3}));
        handler.handle(TestEvents.record(Topics.ACCOUNT_V1, "plain text".getBytes()));

        assertThat(producer.history()).hasSize(2);
        assertThat(producer.history()).allSatisfy(r -> assertThat(r.topic()).isEqualTo(Topics.DLQ));
        assertThat(wiremock.getAllServeEvents()).isEmpty();
    }

    @Test
    void unknownTopicGoesToDlq() {
        handler.handle(TestEvents.record("some.other.topic", TestEvents.account("evt-9", "001A")));

        assertThat(producer.history().get(0).topic()).isEqualTo(Topics.DLQ);
    }
}
