package io.github.i1ich.mskei.ingress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.i1ich.mskei.common.EventHeaders;
import io.github.i1ich.mskei.common.Topics;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EventIngestServiceTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SECRET = "s3cret";

    private static final String VALID = """
            {"ChangeEventHeader":{"entityName":"Account","changeType":"UPDATE",
            "recordIds":["001xx000003DGb2AAG","001xx000003DGb3AAG"],"commitTimestamp":1761556800000,
            "transactionKey":"tx-1","eventId":"evt-123"},"Name":"Acme"}""";

    private MockProducer<String, String> producer;
    private EventIngestService service;

    @BeforeEach
    void setUp() {
        producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        service = new EventIngestService(producer, new IngressConfig(1000, null));
    }

    private static String header(ProducerRecord<String, String> r, String name) {
        return new String(r.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    private static JsonNode json(String s) throws Exception {
        return MAPPER.readTree(s);
    }

    @Test
    void validEventIsPublishedAndAccepted() throws Exception {
        IngressResult result = service.handle(VALID, Map.of());

        assertThat(result.status()).isEqualTo(202);
        JsonNode body = json(result.body());
        assertThat(body.get("eventId").asText()).isEqualTo("evt-123");
        assertThat(body.get("topic").asText()).isEqualTo(Topics.SF_CHANGE_EVENTS_RAW);
        assertThat(body.has("partition")).isTrue();
        assertThat(body.has("offset")).isTrue();

        assertThat(producer.history()).hasSize(1);
        ProducerRecord<String, String> record = producer.history().get(0);
        assertThat(record.topic()).isEqualTo(Topics.SF_CHANGE_EVENTS_RAW);
        assertThat(record.key()).isEqualTo("001xx000003DGb2AAG");
        assertThat(record.value()).isEqualTo(VALID); // raw body, untouched
        assertThat(header(record, EventHeaders.EVENT_ID)).isEqualTo("evt-123");
        assertThat(header(record, EventHeaders.SOURCE)).isEqualTo("salesforce");
        assertThat(header(record, EventHeaders.TRANSACTION_KEY)).isEqualTo("tx-1");
        assertThat(java.time.Instant.parse(header(record, EventHeaders.RECEIVED_AT))).isNotNull();
    }

    @Test
    void missingEventIdGetsGeneratedAndNoTransactionKeyHeader() throws Exception {
        String payload = """
                {"ChangeEventHeader":{"entityName":"Account","changeType":"CREATE",
                "recordIds":["001A"],"commitTimestamp":1}}""";
        IngressResult result = service.handle(payload, Map.of());

        assertThat(result.status()).isEqualTo(202);
        ProducerRecord<String, String> record = producer.history().get(0);
        assertThat(header(record, EventHeaders.EVENT_ID)).isNotBlank();
        assertThat(json(result.body()).get("eventId").asText()).isEqualTo(header(record, EventHeaders.EVENT_ID));
        assertThat(record.headers().lastHeader(EventHeaders.TRANSACTION_KEY)).isNull();
    }

    @Test
    void validationCollectsAllErrorsAndSendsNothing() throws Exception {
        String payload = """
                {"ChangeEventHeader":{"entityName":5,"changeType":"EXPLODE","recordIds":[],"commitTimestamp":"x"}}""";
        IngressResult result = service.handle(payload, Map.of());

        assertThat(result.status()).isEqualTo(400);
        JsonNode body = json(result.body());
        assertThat(body.get("error").asText()).isEqualTo("validation_failed");
        assertThat(body.get("details")).hasSize(4);
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void missingHeaderObjectIsRejected() throws Exception {
        IngressResult result = service.handle("{\"Name\":\"x\"}", Map.of());
        assertThat(result.status()).isEqualTo(400);
        assertThat(json(result.body()).get("details").get(0).asText()).contains("ChangeEventHeader");
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void headerThatIsNotAnObjectIsRejected() {
        assertThat(service.handle("{\"ChangeEventHeader\":\"x\"}", Map.of()).status()).isEqualTo(400);
    }

    @Test
    void recordIdsWithNonStringIsRejected() {
        String payload = """
                {"ChangeEventHeader":{"entityName":"Account","changeType":"CREATE",
                "recordIds":[42],"commitTimestamp":1}}""";
        IngressResult result = service.handle(payload, Map.of());
        assertThat(result.status()).isEqualTo(400);
        assertThat(result.body()).contains("only non-empty strings");
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void recordIdsNotAnArrayIsRejected() {
        String payload = """
                {"ChangeEventHeader":{"entityName":"Account","changeType":"CREATE",
                "recordIds":"001A","commitTimestamp":1}}""";
        assertThat(service.handle(payload, Map.of()).status()).isEqualTo(400);
    }

    @Test
    void malformedJsonIs400() throws Exception {
        IngressResult result = service.handle("{not json", Map.of());
        assertThat(result.status()).isEqualTo(400);
        assertThat(json(result.body()).get("error").asText()).isEqualTo("malformed_json");
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void emptyNullAndNonObjectBodiesAre400() {
        assertThat(service.handle("", Map.of()).status()).isEqualTo(400);
        assertThat(service.handle(null, null).status()).isEqualTo(400);
        assertThat(service.handle("[1,2]", Map.of()).status()).isEqualTo(400);
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void oversizedBodyIs413() {
        EventIngestService small = new EventIngestService(producer, new IngressConfig(10, null));
        IngressResult result = small.handle(VALID, Map.of());
        assertThat(result.status()).isEqualTo(413);
        assertThat(producer.history()).isEmpty();
    }

    // ---- HMAC ----

    private static String sign(String body, String secret) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return "sha256=" + HexFormat.of().formatHex(mac.doFinal(body.getBytes(StandardCharsets.UTF_8)));
    }

    private EventIngestService signedService() {
        return new EventIngestService(producer, new IngressConfig(1000, SECRET));
    }

    @Test
    void hmacMissingHeaderIs401() {
        assertThat(signedService().handle(VALID, Map.of()).status()).isEqualTo(401);
        assertThat(signedService().handle(VALID, null).status()).isEqualTo(401);
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void hmacWrongSignatureIs401() throws Exception {
        assertThat(signedService().handle(VALID, Map.of("X-Signature", sign(VALID, "other"))).status())
                .isEqualTo(401);
        assertThat(signedService().handle(VALID, Map.of("X-Signature", "sha256=zzzz")).status()).isEqualTo(401);
        assertThat(signedService().handle(VALID, Map.of("X-Signature", "md5=abcd")).status()).isEqualTo(401);
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void hmacCorrectSignatureWithAnyHeaderCaseIsAccepted() throws Exception {
        String sig = sign(VALID, SECRET);
        assertThat(signedService().handle(VALID, Map.of("x-signature", sig)).status()).isEqualTo(202);
        assertThat(signedService().handle(VALID, Map.of("X-SIGNATURE", sig.toUpperCase().replace("SHA256=", "sha256=")))
                .status()).isEqualTo(202);
        assertThat(producer.history()).hasSize(2);
    }

    // ---- broker failures ----

    @Test
    void brokerErrorIs503() throws Exception {
        Producer<String, String> failing = mock(Producer.class);
        when(failing.send(any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("boom")));
        IngressResult result = new EventIngestService(failing, new IngressConfig(1000, null)).handle(VALID, Map.of());

        assertThat(result.status()).isEqualTo(503);
        assertThat(json(result.body()).get("error").asText()).isEqualTo("broker_unavailable");
    }

    @Test
    void sendThrowingIs503() {
        Producer<String, String> failing = mock(Producer.class);
        when(failing.send(any())).thenThrow(new org.apache.kafka.common.errors.TimeoutException("metadata"));
        assertThat(new EventIngestService(failing, new IngressConfig(1000, null)).handle(VALID, Map.of()).status())
                .isEqualTo(503);
    }

    @Test
    @SuppressWarnings("unchecked")
    void futureTimeoutIs503() throws Exception {
        Producer<String, String> slow = mock(Producer.class);
        Future<RecordMetadata> future = mock(Future.class);
        when(future.get(any(Long.class), any(TimeUnit.class))).thenThrow(new TimeoutException());
        when(slow.send(any())).thenReturn(future);
        assertThat(new EventIngestService(slow, new IngressConfig(1000, null)).handle(VALID, Map.of()).status())
                .isEqualTo(503);
    }

    @Test
    @SuppressWarnings("unchecked")
    void interruptedWhileWaitingIs503AndKeepsInterruptFlag() throws Exception {
        Producer<String, String> p = mock(Producer.class);
        Future<RecordMetadata> future = mock(Future.class);
        when(future.get(any(Long.class), any(TimeUnit.class))).thenThrow(new InterruptedException());
        when(p.send(any())).thenReturn(future);
        try {
            assertThat(new EventIngestService(p, new IngressConfig(1000, null)).handle(VALID, Map.of()).status())
                    .isEqualTo(503);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted(); // clear the flag so other tests are not affected
        }
    }

    @Test
    void validateReturnsNoErrorsForGoodEvent() throws Exception {
        assertThat(EventIngestService.validate(json(VALID))).isEqualTo(List.of());
    }
}
