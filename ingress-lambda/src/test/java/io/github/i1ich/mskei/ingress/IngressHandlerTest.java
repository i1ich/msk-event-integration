package io.github.i1ich.mskei.ingress;

import static org.assertj.core.api.Assertions.assertThat;

import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IngressHandlerTest {
    private static final String VALID = """
            {"ChangeEventHeader":{"entityName":"Account","changeType":"CREATE",
            "recordIds":["001A"],"commitTimestamp":1}}""";

    private MockProducer<String, String> producer;
    private IngressHandler handler;

    @BeforeEach
    void setUp() {
        producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        handler = new IngressHandler(producer, new IngressConfig(1000, null));
    }

    @Test
    void mapsValidRequestTo202() {
        APIGatewayProxyRequestEvent request = new APIGatewayProxyRequestEvent().withBody(VALID);
        APIGatewayProxyResponseEvent response = handler.handleRequest(request, null);

        assertThat(response.getStatusCode()).isEqualTo(202);
        assertThat(response.getHeaders()).containsEntry("Content-Type", "application/json");
        assertThat(response.getBody()).contains("\"eventId\"").contains("sf.change-events.raw");
        assertThat(producer.history()).hasSize(1);
    }

    @Test
    void decodesBase64Body() {
        String encoded = Base64.getEncoder().encodeToString(VALID.getBytes(StandardCharsets.UTF_8));
        APIGatewayProxyRequestEvent request = new APIGatewayProxyRequestEvent()
                .withBody(encoded).withIsBase64Encoded(true);

        assertThat(handler.handleRequest(request, null).getStatusCode()).isEqualTo(202);
        assertThat(producer.history().get(0).value()).isEqualTo(VALID);
    }

    @Test
    void mapsValidationErrorTo400() {
        APIGatewayProxyResponseEvent response =
                handler.handleRequest(new APIGatewayProxyRequestEvent().withBody("{}"), null);
        assertThat(response.getStatusCode()).isEqualTo(400);
        assertThat(response.getBody()).contains("validation_failed");
    }

    @Test
    void nullBodyIs400() {
        assertThat(handler.handleRequest(new APIGatewayProxyRequestEvent(), null).getStatusCode()).isEqualTo(400);
    }

    @Test
    void passesHeadersForSignatureCheck() {
        IngressHandler signed = new IngressHandler(producer, new IngressConfig(1000, "k"));
        APIGatewayProxyRequestEvent request = new APIGatewayProxyRequestEvent()
                .withBody(VALID).withHeaders(Map.of("X-Signature", "sha256=00"));
        assertThat(signed.handleRequest(request, null).getStatusCode()).isEqualTo(401);
    }
}
