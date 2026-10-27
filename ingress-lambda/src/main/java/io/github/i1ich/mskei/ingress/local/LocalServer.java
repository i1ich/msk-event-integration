package io.github.i1ich.mskei.ingress.local;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.i1ich.mskei.ingress.EventIngestService;
import io.github.i1ich.mskei.ingress.IngressConfig;
import io.github.i1ich.mskei.ingress.IngressResult;
import io.github.i1ich.mskei.ingress.ProducerFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Local stand-in for API Gateway + Lambda: same {@link EventIngestService}, plain JDK HTTP server.
 * Env: INGRESS_PORT (default 8080), INGRESS_API_KEY (optional, mimics an API Gateway API key).
 */
public class LocalServer {
    private static final Logger log = LoggerFactory.getLogger(LocalServer.class);

    private final HttpServer server;
    private final EventIngestService service;
    private final String apiKey;

    public LocalServer(int port, EventIngestService service, String apiKey) throws IOException {
        this.service = service;
        this.apiKey = apiKey;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/events", this::handleEvents);
        server.createContext("/health", ex -> respond(ex, 200, "{\"status\":\"ok\"}"));
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(0);
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private void handleEvents(HttpExchange ex) throws IOException {
        if (!"POST".equals(ex.getRequestMethod())) {
            respond(ex, 405, "{\"error\":\"method_not_allowed\"}");
            return;
        }
        if (apiKey != null && !apiKey.isBlank() && !apiKey.equals(ex.getRequestHeaders().getFirst("x-api-key"))) {
            respond(ex, 403, "{\"message\":\"Forbidden\"}"); // API Gateway answers 403 for a bad key
            return;
        }
        // The service enforces the body size limit.
        String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Map<String, String> headers = new HashMap<>();
        for (Map.Entry<String, List<String>> h : ex.getRequestHeaders().entrySet()) {
            headers.put(h.getKey(), h.getValue().get(0));
        }
        IngressResult result = service.handle(body, headers);
        respond(ex, result.status(), result.body());
    }

    private static void respond(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    public static void main(String[] args) throws IOException {
        Producer<String, String> producer = ProducerFactory.createFromEnv();
        EventIngestService service = new EventIngestService(producer, IngressConfig.fromEnv());
        String portEnv = System.getenv("INGRESS_PORT");
        int port = portEnv == null || portEnv.isBlank() ? 8080 : Integer.parseInt(portEnv.trim());

        LocalServer local = new LocalServer(port, service, System.getenv("INGRESS_API_KEY"));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            local.stop();
            producer.close(); // flushes anything still in flight
        }));
        local.start();
        log.info("Ingress listening on port {}", local.port());
    }
}
