package io.github.i1ich.mskei.bootstrap;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal CloudFormation custom resource protocol.
 *
 * <p>The one rule that matters: a response MUST be sent to the presigned ResponseURL, whatever happens. If the
 * Lambda crashes or times out without answering, CloudFormation waits up to an hour before failing the stack.
 * So the action runs in a worker thread, and we answer FAILED shortly before the Lambda deadline.
 */
final class CustomResource {
    private static final Logger LOG = LoggerFactory.getLogger(CustomResource.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    /** Time kept free before the Lambda deadline to deliver the FAILED response. */
    static final long SAFETY_MARGIN_MS = 10_000;

    private CustomResource() {
    }

    /** The parsed CloudFormation event. */
    record Request(String requestType, String responseUrl, String stackId, String requestId,
                   String logicalResourceId, String physicalResourceId, Map<String, Object> properties) {

        @SuppressWarnings("unchecked")
        static Request parse(Map<String, Object> event) {
            Object props = event.get("ResourceProperties");
            return new Request(
                    str(event, "RequestType"), str(event, "ResponseURL"), str(event, "StackId"),
                    str(event, "RequestId"), str(event, "LogicalResourceId"), str(event, "PhysicalResourceId"),
                    props instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of());
        }

        boolean isDelete() {
            return "Delete".equals(requestType);
        }

        private static String str(Map<String, Object> m, String key) {
            Object v = m.get(key);
            return v == null ? null : v.toString();
        }
    }

    /** What an action returns: the physical id and optional attributes (GetAtt). */
    record Result(String physicalId, Map<String, String> data) {
        static Result of(String physicalId) {
            return new Result(physicalId, Map.of());
        }
    }

    @FunctionalInterface
    interface Action {
        Result run(Request request) throws Exception;
    }

    /** Sends the response body to the presigned URL. Injected so tests need no network. */
    @FunctionalInterface
    interface Responder {
        void send(String responseUrl, String body) throws IOException, InterruptedException;
    }

    /** Real responder: HTTP PUT with an EMPTY Content-Type, as the presigned S3 URL is signed that way. */
    static final class HttpResponder implements Responder {
        private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        @Override
        public void send(String responseUrl, String body) throws IOException, InterruptedException {
            HttpRequest req = HttpRequest.newBuilder(URI.create(responseUrl))
                    .timeout(Duration.ofSeconds(20))
                    .header("Content-Type", "")
                    .PUT(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            LOG.info("CloudFormation response sent, http status {}", resp.statusCode());
        }
    }

    /** Runs the action and always answers CloudFormation (SUCCESS or FAILED). Never throws once a URL is known. */
    static void handle(Map<String, Object> event, Context context, Responder responder, Action action) {
        Request req = Request.parse(event);
        if (req.responseUrl() == null) {
            // Nobody to answer; fail loudly so the invocation shows up as an error.
            throw new IllegalArgumentException("event has no ResponseURL: not a CloudFormation custom resource event");
        }
        LOG.info("custom resource {} {} (logical id {})", req.requestType(), req.stackId(), req.logicalResourceId());

        ExecutorService pool = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "custom-resource-action");
            t.setDaemon(true); // a stuck action must not keep the JVM (and the answer) waiting
            return t;
        });
        Result result = null;
        String failure = null;
        try {
            Future<Result> future = pool.submit(() -> action.run(req));
            long budget = Math.max(1_000, context.getRemainingTimeInMillis() - SAFETY_MARGIN_MS);
            result = future.get(budget, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            failure = "timed out before the Lambda deadline";
        } catch (ExecutionException e) {
            failure = String.valueOf(e.getCause());
            LOG.error("custom resource action failed", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failure = "interrupted";
        } catch (RuntimeException e) {
            failure = String.valueOf(e);
            LOG.error("custom resource handling failed", e);
        } finally {
            pool.shutdownNow();
        }

        String physicalId = result != null ? result.physicalId()
                : req.physicalResourceId() != null ? req.physicalResourceId() : req.logicalResourceId();
        respond(req, responder, failure == null, failure, physicalId, result == null ? Map.of() : result.data());
    }

    private static void respond(Request req, Responder responder, boolean ok, String reason, String physicalId,
                                Map<String, String> data) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("Status", ok ? "SUCCESS" : "FAILED");
        body.put("Reason", ok ? "OK" : (reason == null ? "unknown" : reason));
        body.put("PhysicalResourceId", physicalId == null ? "bootstrap" : physicalId);
        body.put("StackId", req.stackId());
        body.put("RequestId", req.requestId());
        body.put("LogicalResourceId", req.logicalResourceId());
        body.put("Data", data);
        try {
            responder.send(req.responseUrl(), JSON.writeValueAsString(body));
        } catch (IOException e) {
            // Nothing more we can do: CloudFormation will time out. Log so the cause is visible.
            LOG.error("could not send the CloudFormation response", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
