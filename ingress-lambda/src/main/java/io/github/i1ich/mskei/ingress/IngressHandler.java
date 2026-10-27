package io.github.i1ich.mskei.ingress;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.kafka.clients.producer.Producer;

/** AWS Lambda entry point for {@code POST /events} behind API Gateway (proxy integration). */
public class IngressHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    /**
     * Lazy holder: the service (and its Kafka producer) is built on the first invocation of a container
     * and reused by every later invocation, so the expensive connection setup happens once per cold start.
     */
    private static final class Holder {
        static final EventIngestService SERVICE =
                new EventIngestService(ProducerFactory.createFromEnv(), IngressConfig.fromEnv());
    }

    private final EventIngestService testService; // null in production: the holder is used instead

    /** Used by Lambda: no-arg constructor. */
    public IngressHandler() {
        this.testService = null;
    }

    /** Used by tests: inject a mock producer and explicit config. */
    public IngressHandler(Producer<String, String> producer, IngressConfig config) {
        this.testService = new EventIngestService(producer, config);
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        EventIngestService service = testService != null ? testService : Holder.SERVICE;

        String body = request.getBody();
        // API Gateway may base64-encode the body; the signature is over the decoded bytes.
        if (Boolean.TRUE.equals(request.getIsBase64Encoded()) && body != null) {
            body = new String(Base64.getDecoder().decode(body), StandardCharsets.UTF_8);
        }
        IngressResult result = service.handle(body, request.getHeaders());

        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        response.setStatusCode(result.status());
        response.setHeaders(Map.of("Content-Type", "application/json"));
        response.setBody(result.body());
        return response;
    }
}
