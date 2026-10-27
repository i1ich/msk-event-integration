package io.github.i1ich.mskei.it.support;

import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import org.awaitility.Awaitility;

/** Queries what the in-process WireMock (the downstream system) has received. */
public final class Downstream {
    private static final ObjectMapper JSON = new ObjectMapper();

    private Downstream() {
    }

    /** A received POST and its parsed JSON body. */
    public record Call(LoggedRequest request, JsonNode body) {
        public String idempotencyKey() {
            return request.getHeader("Idempotency-Key");
        }
    }

    /** Calls to {@code path} (e.g. /downstream/accounts) whose body matches, oldest first. */
    public static List<Call> calls(WireMockServer wm, String path, Predicate<JsonNode> bodyMatches) {
        return wm.findAll(postRequestedFor(urlEqualTo(path))).stream()
                .map(r -> new Call(r, parse(r.getBodyAsString())))
                .filter(c -> bodyMatches.test(c.body()))
                .sorted(Comparator.comparing(c -> c.request().getLoggedDate()))
                .toList();
    }

    /** Waits until at least {@code count} matching calls have arrived and returns them, oldest first. */
    public static List<Call> awaitCalls(WireMockServer wm, String path, Predicate<JsonNode> bodyMatches, int count) {
        Awaitility.await("downstream calls to " + path).until(() -> calls(wm, path, bodyMatches).size() >= count);
        return calls(wm, path, bodyMatches);
    }

    private static JsonNode parse(String body) {
        try {
            return JSON.readTree(body);
        } catch (IOException e) {
            throw new IllegalStateException("Downstream body is not JSON: " + body, e);
        }
    }
}
