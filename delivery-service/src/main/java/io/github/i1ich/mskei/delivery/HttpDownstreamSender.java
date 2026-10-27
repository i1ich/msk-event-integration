package io.github.i1ich.mskei.delivery;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/** {@link DownstreamSender} on top of the JDK HttpClient. */
public final class HttpDownstreamSender implements DownstreamSender {
    private final HttpClient client;
    private final String baseUrl;
    private final Duration requestTimeout;

    public HttpDownstreamSender(String baseUrl, Duration connectTimeout, Duration requestTimeout) {
        this.client = HttpClient.newBuilder().connectTimeout(connectTimeout).build();
        // Strip a trailing slash so base + "/accounts" never becomes "//accounts".
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.requestTimeout = requestTimeout;
    }

    @Override
    public int post(String path, String idempotencyKey, String json) throws IOException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(requestTimeout)
                .header("Content-Type", "application/json")
                // The downstream deduplicates on this key, which makes our at-least-once delivery safe.
                .header("Idempotency-Key", idempotencyKey)
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                .build();
        try {
            return client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (InterruptedException e) {
            // Shutdown while waiting: keep the interrupt flag and treat it as a retryable failure.
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while calling downstream", e);
        }
    }
}
