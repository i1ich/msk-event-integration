package io.github.i1ich.mskei.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.i1ich.mskei.common.EventHeaders;
import io.github.i1ich.mskei.common.Topics;
import io.github.i1ich.mskei.ingress.IngressResult;
import io.github.i1ich.mskei.it.support.Downstream;
import io.github.i1ich.mskei.it.support.Events;
import io.github.i1ich.mskei.it.support.TopicTail;
import io.github.i1ich.mskei.schema.AccountEvent;
import io.github.i1ich.mskei.schema.ChangeType;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/** Salesforce JSON -> ingress -> raw topic -> stream-processor -> Avro topic -> delivery-service -> HTTP. */
class IngressToDownstreamIT extends AbstractIT {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void validAccountEventIsAcceptedMappedAndDeliveredWithIdempotencyKey() throws Exception {
        String accountId = Events.accountId();
        String body = Events.account("CREATE", accountId, Map.of(
                "Name", "Acme Corp", "Phone", "+1-555-0100", "BillingCity", "Springfield",
                "Industry", "Manufacturing"));

        IngressResult result = ENV.ingress().handle(body, Map.of());

        assertThat(result.status()).isEqualTo(202);
        String eventId = JSON.readTree(result.body()).get("eventId").asText();

        try (TopicTail raw = ENV.tail(Topics.SF_CHANGE_EVENTS_RAW);
                TopicTail accounts = ENV.tail(Topics.ACCOUNT_V1)) {
            // 1. raw topic: original JSON bytes, key = record id, headers set by ingress
            ConsumerRecord<byte[], byte[]> rawRecord = raw.awaitOne(TopicTail.keyIs(accountId));
            assertThat(new String(rawRecord.value(), StandardCharsets.UTF_8)).isEqualTo(body);
            assertThat(TopicTail.header(rawRecord, EventHeaders.EVENT_ID)).isEqualTo(eventId);
            assertThat(TopicTail.header(rawRecord, EventHeaders.SOURCE)).isEqualTo("salesforce");
            assertThat(TopicTail.header(rawRecord, EventHeaders.RECEIVED_AT)).isNotBlank();
            assertThat(TopicTail.header(rawRecord, EventHeaders.TRANSACTION_KEY)).isNotBlank();

            // 2. canonical topic: Avro, schema from the registry, specific reader
            ConsumerRecord<byte[], byte[]> mapped = accounts.awaitOne(TopicTail.keyIs(accountId));
            AccountEvent event = (AccountEvent) ENV.avroDeserializer().deserialize(Topics.ACCOUNT_V1, mapped.value());
            assertThat(event.getEventId()).isEqualTo(eventId);
            assertThat(event.getAccountId()).isEqualTo(accountId);
            assertThat(event.getChangeType()).isEqualTo(ChangeType.CREATE);
            assertThat(event.getName()).isEqualTo("Acme Corp");
            assertThat(event.getPhone()).isEqualTo("+1-555-0100");
            assertThat(event.getBillingCity()).isEqualTo("Springfield");
            assertThat(event.getIndustry()).isEqualTo("Manufacturing"); // v2 field
            assertThat(event.getSourceSystem()).isEqualTo("salesforce");
            assertThat(event.getTransactionKey())
                    .isEqualTo(JSON.readTree(body).at("/ChangeEventHeader/transactionKey").asText());
        }

        // 3. downstream: one POST, Idempotency-Key = event id
        List<Downstream.Call> calls = Downstream.awaitCalls(ENV.wireMock(), "/downstream/accounts",
                b -> accountId.equals(b.path("accountId").asText()), 1);
        assertThat(calls).hasSize(1);
        JsonNode sent = calls.get(0).body();
        assertThat(calls.get(0).idempotencyKey()).isEqualTo(eventId);
        assertThat(sent.path("eventId").asText()).isEqualTo(eventId);
        assertThat(sent.path("industry").asText()).isEqualTo("Manufacturing");
    }
}
