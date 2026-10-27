package io.github.i1ich.mskei.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.i1ich.mskei.common.DeadLetters;
import io.github.i1ich.mskei.common.EventHeaders;
import io.github.i1ich.mskei.schema.AccountEvent;
import io.github.i1ich.mskei.schema.ChangeType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.header.Headers;

/**
 * Topology A mapping: Salesforce Change Data Capture JSON (as stored by ingress-lambda) to {@link AccountEvent}.
 *
 * <p>Salesforce can batch several records of one transaction into ONE change event ({@code recordIds} has many
 * entries). We emit one AccountEvent per record id, so downstream always deals with one account per event. They
 * all share the same field values (a batched event carries the same changed fields for every id). When there is
 * more than one, eventId gets a "-&lt;index&gt;" suffix so that eventIds (used as Idempotency-Key) stay unique.
 */
public class AccountMapper implements RecordMapper<AccountEvent> {
    static final String ENTITY = "Account";

    private final ObjectMapper json = new ObjectMapper();

    @Override
    public List<Mapped<AccountEvent>> map(byte[] value, Headers headers, DeadLetters.Source source)
            throws Exception {
        if (value == null) {
            throw new IllegalArgumentException("Empty value");
        }
        JsonNode root = json.readTree(value);
        JsonNode header = root.path("ChangeEventHeader");
        if (!header.isObject()) {
            throw new IllegalArgumentException("Missing ChangeEventHeader");
        }

        String entity = requiredText(header, "entityName");
        if (!ENTITY.equals(entity)) {
            throw new UnmappedEntityException(entity);
        }

        ChangeType changeType = ChangeType.valueOf(requiredText(header, "changeType"));
        JsonNode recordIds = header.path("recordIds");
        if (!recordIds.isArray() || recordIds.isEmpty()) {
            throw new IllegalArgumentException("ChangeEventHeader.recordIds is missing or empty");
        }
        if (!header.path("commitTimestamp").isNumber()) {
            throw new IllegalArgumentException("ChangeEventHeader.commitTimestamp is missing");
        }
        Instant commitTimestamp = Instant.ofEpochMilli(header.get("commitTimestamp").asLong());

        // Ingress sets event-id; the fallback only matters for records written by something else.
        String baseEventId = DeadLetters.header(headers, EventHeaders.EVENT_ID);
        if (baseEventId == null) {
            baseEventId = source.topic() + "-" + source.partition() + "-" + source.offset();
        }

        List<Mapped<AccountEvent>> result = new ArrayList<>();
        for (int i = 0; i < recordIds.size(); i++) {
            String accountId = recordIds.get(i).asText();
            String eventId = recordIds.size() == 1 ? baseEventId : baseEventId + "-" + i;
            AccountEvent event = AccountEvent.newBuilder()
                    .setEventId(eventId)
                    .setAccountId(accountId)
                    .setChangeType(changeType)
                    .setName(text(root, "Name"))
                    .setPhone(text(root, "Phone"))
                    .setBillingCity(text(root, "BillingCity"))
                    .setIndustry(text(root, "Industry"))
                    .setTransactionKey(text(header, "transactionKey"))
                    .setCommitTimestamp(commitTimestamp)
                    .setSourceSystem("salesforce")
                    .build();
            result.add(new Mapped<>(accountId, event));
        }
        return result;
    }

    private static String requiredText(JsonNode node, String field) {
        String v = text(node, field);
        if (v == null) {
            throw new IllegalArgumentException("ChangeEventHeader." + field + " is missing");
        }
        return v;
    }

    /** Text of a field, or null if absent / JSON null (e.g. a DELETE carries no field values). */
    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }
}
