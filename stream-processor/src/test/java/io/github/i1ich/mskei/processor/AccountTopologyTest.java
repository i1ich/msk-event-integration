package io.github.i1ich.mskei.processor;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.i1ich.mskei.common.EventHeaders;
import io.github.i1ich.mskei.common.Topics;
import io.github.i1ich.mskei.schema.AccountEvent;
import io.github.i1ich.mskei.schema.ChangeType;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

class AccountTopologyTest extends TopologyTestBase {

    private static final String CREATE = """
            {"ChangeEventHeader":{"entityName":"Account","changeType":"CREATE","recordIds":["001A"],
             "commitTimestamp":1761556800000,"transactionKey":"tx-1"},
             "Name":"Acme Corp","Phone":"+1-555-0100","BillingCity":"Springfield","Industry":"Retail"}""";

    @Test
    void createIsMappedAndHeadersAreKept() {
        TestOutputTopic<String, AccountEvent> out = output(Topics.ACCOUNT_V1);

        salesforceIn.pipeInput(sf("k", CREATE, headers("evt-1")));

        TestRecord<String, AccountEvent> r = out.readRecord();
        assertThat(r.key()).isEqualTo("001A");
        assertThat(r.value()).isEqualTo(AccountEvent.newBuilder()
                .setEventId("evt-1").setAccountId("001A").setChangeType(ChangeType.CREATE)
                .setName("Acme Corp").setPhone("+1-555-0100").setBillingCity("Springfield").setIndustry("Retail")
                .setTransactionKey("tx-1").setCommitTimestamp(Instant.ofEpochMilli(1761556800000L))
                .setSourceSystem("salesforce").build());
        assertThat(header(r, EventHeaders.EVENT_ID)).isEqualTo("evt-1");
        assertThat(header(r, EventHeaders.SOURCE)).isEqualTo("salesforce");
        assertThat(dlqOut.isEmpty()).isTrue();
    }

    @Test
    void updateKeepsOnlyTheChangedFields() {
        TestOutputTopic<String, AccountEvent> out = output(Topics.ACCOUNT_V1);
        String json = """
                {"ChangeEventHeader":{"entityName":"Account","changeType":"UPDATE","recordIds":["001A"],
                 "commitTimestamp":1761556860000,"transactionKey":"tx-2"},"Phone":"+1-555-0199"}""";

        salesforceIn.pipeInput(sf("k", json, headers("evt-2")));

        AccountEvent e = out.readValue();
        assertThat(e.getChangeType()).isEqualTo(ChangeType.UPDATE);
        assertThat(e.getPhone()).isEqualTo("+1-555-0199");
        assertThat(e.getName()).isNull();
        assertThat(e.getBillingCity()).isNull();
    }

    @Test
    void deleteHasNoFieldValues() {
        TestOutputTopic<String, AccountEvent> out = output(Topics.ACCOUNT_V1);
        String json = """
                {"ChangeEventHeader":{"entityName":"Account","changeType":"DELETE","recordIds":["001A"],
                 "commitTimestamp":1761556920000,"transactionKey":"tx-3"}}""";

        salesforceIn.pipeInput(sf("k", json, headers("evt-3")));

        AccountEvent e = out.readValue();
        assertThat(e.getChangeType()).isEqualTo(ChangeType.DELETE);
        assertThat(e.getName()).isNull();
    }

    @Test
    void severalRecordIdsProduceOneEventEachWithUniqueEventIds() {
        TestOutputTopic<String, AccountEvent> out = output(Topics.ACCOUNT_V1);
        String json = """
                {"ChangeEventHeader":{"entityName":"Account","changeType":"UPDATE","recordIds":["001A","001B"],
                 "commitTimestamp":1761556860000},"Industry":"Banking"}""";

        salesforceIn.pipeInput(sf("k", json, headers("evt-4")));

        List<TestRecord<String, AccountEvent>> records = out.readRecordsToList();
        assertThat(records).extracting(TestRecord::key).containsExactly("001A", "001B");
        assertThat(records).extracting(r -> r.value().getEventId()).containsExactly("evt-4-0", "evt-4-1");
        assertThat(records).extracting(r -> r.value().getIndustry()).containsOnly("Banking");
        assertThat(records).allSatisfy(r -> assertThat(header(r, EventHeaders.EVENT_ID)).isEqualTo("evt-4"));
    }

    @Test
    void missingEventIdHeaderFallsBackToTopicPartitionOffset() {
        TestOutputTopic<String, AccountEvent> out = output(Topics.ACCOUNT_V1);

        salesforceIn.pipeInput(sf("k", CREATE, new org.apache.kafka.common.header.internals.RecordHeaders()));

        assertThat(out.readValue().getEventId()).isEqualTo(Topics.SF_CHANGE_EVENTS_RAW + "-0-0");
    }

    @Test
    void unknownEntityGoesToDlqWithErrorHeadersAndOriginalBytes() {
        String json = """
                {"ChangeEventHeader":{"entityName":"Opportunity","changeType":"CREATE","recordIds":["006X"],
                 "commitTimestamp":1761556980000},"Name":"Big Deal"}""";
        byte[] original = json.getBytes(StandardCharsets.UTF_8);

        salesforceIn.pipeInput(sf("k", json, headers("evt-5")));

        TestRecord<byte[], byte[]> dlq = dlqOut.readRecord();
        assertThat(dlq.value()).isEqualTo(original);
        assertThat(dlq.key()).isEqualTo("k".getBytes(StandardCharsets.UTF_8));
        assertThat(header(dlq, EventHeaders.ERROR_CLASS)).isEqualTo(UnmappedEntityException.class.getName());
        assertThat(header(dlq, EventHeaders.ERROR_MESSAGE)).contains("Opportunity");
        assertThat(header(dlq, EventHeaders.SOURCE_TOPIC)).isEqualTo(Topics.SF_CHANGE_EVENTS_RAW);
        assertThat(header(dlq, EventHeaders.SOURCE_PARTITION)).isEqualTo("0");
        assertThat(header(dlq, EventHeaders.SOURCE_OFFSET)).isEqualTo("0");
        assertThat(header(dlq, EventHeaders.ATTEMPTS)).isEqualTo("1");
        assertThat(header(dlq, EventHeaders.FAILED_AT)).isNotNull();
        // original headers survive
        assertThat(header(dlq, EventHeaders.EVENT_ID)).isEqualTo("evt-5");
        assertThat(output(Topics.ACCOUNT_V1).isEmpty()).isTrue();
    }

    @Test
    void malformedJsonGoesToDlqAndProcessingContinues() {
        TestOutputTopic<String, AccountEvent> out = output(Topics.ACCOUNT_V1);

        salesforceIn.pipeInput(sf("k", "{not json", headers("bad")));
        salesforceIn.pipeInput(sf("k", CREATE, headers("good")));

        TestRecord<byte[], byte[]> dlq = dlqOut.readRecord();
        assertThat(header(dlq, EventHeaders.SOURCE_OFFSET)).isEqualTo("0");
        assertThat(out.readValue().getEventId()).isEqualTo("good");
        assertThat(dlqOut.isEmpty()).isTrue();
    }

    @Test
    void invalidContentGoesToDlq() {
        String noHeader = "{\"Name\":\"x\"}";
        String noRecordIds = """
                {"ChangeEventHeader":{"entityName":"Account","changeType":"CREATE","recordIds":[],
                 "commitTimestamp":1}}""";
        String badChangeType = """
                {"ChangeEventHeader":{"entityName":"Account","changeType":"GAP_UPDATE","recordIds":["1"],
                 "commitTimestamp":1}}""";
        String noTimestamp = """
                {"ChangeEventHeader":{"entityName":"Account","changeType":"CREATE","recordIds":["1"]}}""";
        String noEntity = """
                {"ChangeEventHeader":{"changeType":"CREATE","recordIds":["1"],"commitTimestamp":1}}""";

        for (String json : List.of(noHeader, noRecordIds, badChangeType, noTimestamp, noEntity)) {
            salesforceIn.pipeInput(sf(null, json, headers("e")));
        }

        assertThat(dlqOut.readRecordsToList()).hasSize(5)
                .allSatisfy(r -> assertThat(r.key()).isNull());
        assertThat(output(Topics.ACCOUNT_V1).isEmpty()).isTrue();
    }

    @Test
    void nullValueGoesToDlq() {
        salesforceIn.pipeInput(new TestRecord<String, byte[]>("k", null, headers("e"), 1L));

        assertThat(dlqOut.readRecord().value()).isNull();
    }
}
