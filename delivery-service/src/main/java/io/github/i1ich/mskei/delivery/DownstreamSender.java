package io.github.i1ich.mskei.delivery;

import java.io.IOException;

/** The one thing the handler needs from HTTP. An interface so tests can use WireMock or a lambda. */
@FunctionalInterface
public interface DownstreamSender {
    /**
     * POSTs a JSON body to {@code path} and returns the HTTP status.
     *
     * @throws IOException on timeouts and network errors
     */
    int post(String path, String idempotencyKey, String json) throws IOException;
}
