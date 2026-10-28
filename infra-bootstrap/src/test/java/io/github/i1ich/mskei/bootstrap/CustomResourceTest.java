package io.github.i1ich.mskei.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.lambda.runtime.Context;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

class CustomResourceTest {
    private final List<String> bodies = new ArrayList<>();

    private static Context context(long remainingMs) {
        Context c = mock(Context.class);
        when(c.getRemainingTimeInMillis()).thenReturn((int) remainingMs);
        return c;
    }

    private static Map<String, Object> event(String url) {
        return Map.of("RequestType", "Create", "ResponseURL", url, "StackId", "s", "RequestId", "r",
                "LogicalResourceId", "L", "PhysicalResourceId", "P");
    }

    @Test
    void successCarriesPhysicalIdAndData() {
        CustomResource.handle(event("http://x"), context(60_000), (u, b) -> bodies.add(b),
                req -> new CustomResource.Result("pid", Map.of("k", "v")));
        assertThat(bodies.get(0)).contains("SUCCESS").contains("\"PhysicalResourceId\":\"pid\"").contains("\"k\":\"v\"");
    }

    @Test
    void exceptionBecomesFailedKeepingPreviousPhysicalId() {
        CustomResource.handle(event("http://x"), context(60_000), (u, b) -> bodies.add(b),
                req -> {
                    throw new IllegalStateException("bad");
                });
        assertThat(bodies.get(0)).contains("FAILED").contains("bad").contains("\"PhysicalResourceId\":\"P\"");
    }

    @Test
    void slowActionIsAnsweredFailedBeforeDeadline() {
        // remaining = margin + 1 s -> budget 1 s; action sleeps much longer
        CustomResource.handle(event("http://x"), context(CustomResource.SAFETY_MARGIN_MS + 1_000),
                (u, b) -> bodies.add(b), req -> {
                    Thread.sleep(30_000);
                    return CustomResource.Result.of("never");
                });
        assertThat(bodies.get(0)).contains("FAILED").contains("timed out");
    }

    @Test
    void responderFailureDoesNotThrow() {
        CustomResource.handle(event("http://x"), context(60_000), (u, b) -> {
            throw new IOException("net down");
        }, req -> CustomResource.Result.of("p"));
    }

    @Test
    void missingResponseUrlIsRejected() {
        assertThatThrownBy(() -> CustomResource.handle(Map.of("RequestType", "Create"), context(1000),
                (u, b) -> { }, req -> CustomResource.Result.of("p")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void httpResponderPutsWithEmptyContentType() throws Exception {
        List<String> got = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/resp", ex -> {
            got.add(ex.getRequestMethod() + "|" + ex.getRequestHeaders().getFirst("Content-Type") + "|"
                    + new String(ex.getRequestBody().readAllBytes()));
            ex.sendResponseHeaders(200, -1);
            ex.close();
        });
        server.start();
        try {
            new CustomResource.HttpResponder().send("http://127.0.0.1:" + server.getAddress().getPort() + "/resp", "{}");
        } finally {
            server.stop(0);
        }
        assertThat(got).hasSize(1);
        assertThat(got.get(0)).startsWith("PUT|").endsWith("|{}");
        assertThat(got.get(0).split("\\|")[1]).isIn("", "null");
    }
}
