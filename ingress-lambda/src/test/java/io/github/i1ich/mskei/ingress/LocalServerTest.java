package io.github.i1ich.mskei.ingress;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.i1ich.mskei.ingress.local.LocalServer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LocalServerTest {
    private static final String VALID = """
            {"ChangeEventHeader":{"entityName":"Account","changeType":"CREATE",
            "recordIds":["001A"],"commitTimestamp":1}}""";

    private final HttpClient http = HttpClient.newHttpClient();
    private MockProducer<String, String> producer;
    private LocalServer server;
    private String base;

    @BeforeEach
    void start() throws Exception {
        producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        EventIngestService service = new EventIngestService(producer, new IngressConfig(1000, null));
        server = new LocalServer(0, service, "key-1"); // port 0 = any free port
        server.start();
        base = "http://localhost:" + server.port();
    }

    @AfterEach
    void stop() {
        server.stop();
    }

    private HttpResponse<String> post(String body, String apiKey) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + "/events"))
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (apiKey != null) {
            b.header("x-api-key", apiKey);
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void healthIsOk() throws Exception {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(base + "/health")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(r.statusCode()).isEqualTo(200);
        assertThat(r.body()).contains("ok");
    }

    @Test
    void postWithApiKeyIsAccepted() throws Exception {
        HttpResponse<String> r = post(VALID, "key-1");
        assertThat(r.statusCode()).isEqualTo(202);
        assertThat(producer.history()).hasSize(1);
    }

    @Test
    void postWithoutOrWithWrongApiKeyIs403() throws Exception {
        assertThat(post(VALID, null).statusCode()).isEqualTo(403);
        assertThat(post(VALID, "nope").statusCode()).isEqualTo(403);
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void invalidBodyIs400() throws Exception {
        assertThat(post("{}", "key-1").statusCode()).isEqualTo(400);
    }

    @Test
    void getOnEventsIs405() throws Exception {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(base + "/events")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(r.statusCode()).isEqualTo(405);
    }
}
