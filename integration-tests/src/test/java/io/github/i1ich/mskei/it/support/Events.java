package io.github.i1ich.mskei.it.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Builders for test input. Every test generates its own record ids, so tests do not depend on each other. */
public final class Events {
    private static final ObjectMapper JSON = new ObjectMapper();

    private Events() {
    }

    /** A Salesforce-looking 18 character id, unique per call: 001 + 12 hex + AAA. */
    public static String accountId() {
        return "001" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase() + "AAA";
    }

    /** A Salesforce Change Data Capture event as ingress-lambda receives it. */
    public static String salesforce(String entity, String changeType, String recordId, Map<String, ?> fields) {
        ObjectNode root = JSON.createObjectNode();
        ObjectNode header = root.putObject("ChangeEventHeader");
        header.put("entityName", entity);
        header.put("changeType", changeType);
        ArrayNode ids = header.putArray("recordIds");
        ids.add(recordId);
        header.put("commitTimestamp", Instant.now().toEpochMilli());
        header.put("transactionKey", UUID.randomUUID().toString());
        header.put("changeOrigin", "com/salesforce/api/rest/61.0");
        header.put("commitUser", "005xx000001Sv6AAAS");
        fields.forEach((k, v) -> root.putPOJO(k, v));
        return root.toString();
    }

    public static String account(String changeType, String accountId, Map<String, ?> fields) {
        return salesforce("Account", changeType, accountId, fields);
    }
}
