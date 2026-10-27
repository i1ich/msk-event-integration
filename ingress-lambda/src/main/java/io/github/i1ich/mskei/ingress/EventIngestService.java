package io.github.i1ich.mskei.ingress;

import static net.logstash.logback.argument.StructuredArguments.kv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.i1ich.mskei.common.EventHeaders;
import io.github.i1ich.mskei.common.Topics;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The whole ingress logic, with no dependency on Lambda or HTTP server types:
 * size check, signature check, parse, validate, publish to Kafka.
 * Nothing is published unless every check passes.
 */
public class EventIngestService {
    private static final Logger log = LoggerFactory.getLogger(EventIngestService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<String> CHANGE_TYPES = List.of("CREATE", "UPDATE", "DELETE", "UNDELETE");
    private static final long SEND_TIMEOUT_SECONDS = 10;
    public static final String SIGNATURE_HEADER = "X-Signature";

    private final Producer<String, String> producer;
    private final IngressConfig config;

    public EventIngestService(Producer<String, String> producer, IngressConfig config) {
        this.producer = producer;
        this.config = config;
    }

    /**
     * @param rawBody the request body exactly as received (it is what gets signed and what is published)
     * @param headers HTTP request headers; looked up case-insensitively
     */
    public IngressResult handle(String rawBody, Map<String, String> headers) {
        String body = rawBody == null ? "" : rawBody;
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);

        // 1. Size limit
        if (bytes.length > config.maxBodyBytes()) {
            log.warn("Rejected: body too large", kv("bytes", bytes.length), kv("limit", config.maxBodyBytes()));
            return error(413, "payload_too_large");
        }

        // 2. Optional HMAC signature
        if (config.hmacEnabled() && !signatureValid(bytes, headerValue(headers, SIGNATURE_HEADER))) {
            log.warn("Rejected: missing or invalid signature");
            return error(401, "invalid_signature");
        }

        // 3. Parse JSON
        JsonNode root;
        try {
            root = MAPPER.readTree(body);
        } catch (JsonProcessingException e) {
            log.warn("Rejected: malformed JSON");
            return error(400, "malformed_json");
        }
        if (root == null || !root.isObject()) {
            // Empty body, or a JSON value that is not an object.
            return validationFailed(List.of("body must be a JSON object"));
        }

        // 4. Validate (collect all errors)
        List<String> errors = validate(root);
        if (!errors.isEmpty()) {
            log.warn("Rejected: validation failed", kv("errors", errors));
            return validationFailed(errors);
        }

        // 5. Publish
        return publish(body, root.get("ChangeEventHeader"));
    }

    /** Returns every problem found in the ChangeEventHeader; an empty list means valid. */
    static List<String> validate(JsonNode root) {
        List<String> errors = new ArrayList<>();
        JsonNode header = root.get("ChangeEventHeader");
        if (header == null || !header.isObject()) {
            errors.add("ChangeEventHeader is required and must be an object");
            return errors;
        }
        if (!header.path("entityName").isTextual() || header.path("entityName").asText().isBlank()) {
            errors.add("ChangeEventHeader.entityName is required and must be a non-empty string");
        }
        JsonNode changeType = header.get("changeType");
        if (changeType == null || !changeType.isTextual() || !CHANGE_TYPES.contains(changeType.asText())) {
            errors.add("ChangeEventHeader.changeType must be one of " + CHANGE_TYPES);
        }
        JsonNode ids = header.get("recordIds");
        if (ids == null || !ids.isArray() || ids.isEmpty()) {
            errors.add("ChangeEventHeader.recordIds must be a non-empty array of strings");
        } else {
            for (JsonNode id : ids) {
                if (!id.isTextual() || id.asText().isBlank()) {
                    errors.add("ChangeEventHeader.recordIds must contain only non-empty strings");
                    break;
                }
            }
        }
        if (!header.path("commitTimestamp").isNumber()) {
            errors.add("ChangeEventHeader.commitTimestamp is required and must be a number");
        }
        return errors;
    }

    private IngressResult publish(String body, JsonNode header) {
        String key = header.get("recordIds").get(0).asText();
        JsonNode idNode = header.path("eventId");
        String eventId = idNode.isTextual() && !idNode.asText().isBlank()
                ? idNode.asText()
                : UUID.randomUUID().toString();

        ProducerRecord<String, String> record = new ProducerRecord<>(Topics.SF_CHANGE_EVENTS_RAW, key, body);
        addHeader(record, EventHeaders.EVENT_ID, eventId);
        addHeader(record, EventHeaders.RECEIVED_AT, Instant.now().toString());
        addHeader(record, EventHeaders.SOURCE, "salesforce");
        if (header.path("transactionKey").isTextual()) {
            addHeader(record, EventHeaders.TRANSACTION_KEY, header.get("transactionKey").asText());
        }

        try {
            // Wait for the broker ack so we only answer 202 once the event is safely stored.
            RecordMetadata md = producer.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("Published", kv("eventId", eventId), kv("topic", md.topic()),
                    kv("partition", md.partition()), kv("offset", md.offset()));
            ObjectNode ok = MAPPER.createObjectNode();
            ok.put("eventId", eventId);
            ok.put("topic", md.topic());
            ok.put("partition", md.partition());
            ok.put("offset", md.offset());
            return new IngressResult(202, ok.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Publish interrupted", kv("eventId", eventId), e);
            return error(503, "broker_unavailable");
        } catch (Exception e) {
            // ExecutionException (broker error), TimeoutException, or a failure thrown by send() itself.
            log.error("Publish failed", kv("eventId", eventId), e);
            return error(503, "broker_unavailable");
        }
    }

    private static void addHeader(ProducerRecord<String, String> record, String name, String value) {
        record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }

    /** Expects {@code sha256=<hex>} where hex is HMAC-SHA256(secret, raw body). */
    private boolean signatureValid(byte[] body, String headerValue) {
        if (headerValue == null || !headerValue.startsWith("sha256=")) {
            return false;
        }
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(config.hmacSecret().getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] expected = mac.doFinal(body);
            byte[] given = HexFormat.of().parseHex(headerValue.substring("sha256=".length()).toLowerCase());
            return MessageDigest.isEqual(expected, given); // constant-time comparison
        } catch (IllegalArgumentException e) {
            return false; // not valid hex
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e); // HmacSHA256 always exists on a JVM
        }
    }

    private static String headerValue(Map<String, String> headers, String name) {
        if (headers == null) {
            return null;
        }
        Map<String, String> ci = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        ci.putAll(headers);
        return ci.get(name);
    }

    private static IngressResult error(int status, String code) {
        return new IngressResult(status, MAPPER.createObjectNode().put("error", code).toString());
    }

    private static IngressResult validationFailed(List<String> details) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("error", "validation_failed");
        ArrayNode arr = node.putArray("details");
        details.forEach(arr::add);
        return new IngressResult(400, node.toString());
    }
}
