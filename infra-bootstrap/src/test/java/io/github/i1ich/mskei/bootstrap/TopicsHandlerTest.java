package io.github.i1ich.mskei.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.lambda.runtime.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TopicsHandlerTest {
    private final List<String> responses = new ArrayList<>();
    private final CustomResource.Responder responder = (url, body) -> responses.add(url + "|" + body);

    private static Context context(long remainingMs) {
        Context c = mock(Context.class);
        when(c.getRemainingTimeInMillis()).thenReturn((int) remainingMs);
        return c;
    }

    private static Map<String, Object> event(String type, Map<String, Object> props) {
        return Map.of("RequestType", type, "ResponseURL", "https://cfn.example/resp", "StackId", "stack",
                "RequestId", "req", "LogicalResourceId", "Topics", "ResourceProperties", props);
    }

    private static CreateTopicsResult resultFor(String name, Throwable error) {
        KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
        if (error == null) {
            f.complete(null);
        } else {
            f.completeExceptionally(error);
        }
        CreateTopicsResult r = mock(CreateTopicsResult.class);
        when(r.values()).thenReturn(Map.of(name, f));
        return r;
    }

    private static final List<TopicsHandler.TopicSpec> SPECS = List.of(
            new TopicsHandler.TopicSpec("a", 3, 1000), new TopicsHandler.TopicSpec("b", 3, 2000));

    @Test
    void createsTopicsWithDefaultsAndBrokerReplication() {
        Admin admin = mock(Admin.class);
        when(admin.createTopics(anyCollection())).thenAnswer(inv -> {
            NewTopic t = inv.<List<NewTopic>>getArgument(0).get(0);
            return resultFor(t.name(), null);
        });
        new TopicsHandler(() -> admin, SPECS, responder)
                .handleRequest(event("Create", Map.of("ReplicationFactor", "-1")), context(60_000));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<NewTopic>> cap = ArgumentCaptor.forClass(List.class);
        Mockito.verify(admin, Mockito.times(2)).createTopics(cap.capture());
        NewTopic first = cap.getAllValues().get(0).get(0);
        assertThat(first.numPartitions()).isEqualTo(3);
        assertThat(first.replicationFactor()).isEqualTo((short) -1);
        assertThat(first.configs()).containsEntry("retention.ms", "1000");
        assertThat(responses).hasSize(1);
        assertThat(responses.get(0)).contains("\"Status\":\"SUCCESS\"").contains("\"PhysicalResourceId\":\"topics\"");
    }

    @Test
    void partitionsAndReplicationOverridesAreApplied() {
        Admin admin = mock(Admin.class);
        when(admin.createTopics(anyCollection())).thenAnswer(inv ->
                resultFor(inv.<List<NewTopic>>getArgument(0).get(0).name(), null));
        new TopicsHandler(() -> admin, SPECS, responder)
                .handleRequest(event("Update", Map.of("Partitions", 6, "ReplicationFactor", "2")), context(60_000));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<NewTopic>> cap = ArgumentCaptor.forClass(List.class);
        Mockito.verify(admin, Mockito.times(2)).createTopics(cap.capture());
        NewTopic t = cap.getAllValues().get(0).get(0);
        assertThat(t.numPartitions()).isEqualTo(6);
        assertThat(t.replicationFactor()).isEqualTo((short) 2);
    }

    @Test
    void existingTopicsAreLeftAlone() {
        Admin admin = mock(Admin.class);
        when(admin.createTopics(anyCollection())).thenAnswer(inv ->
                resultFor(inv.<List<NewTopic>>getArgument(0).get(0).name(), new TopicExistsException("x")));
        new TopicsHandler(() -> admin, SPECS, responder).handleRequest(event("Create", Map.of()), context(60_000));
        assertThat(responses.get(0)).contains("SUCCESS").contains("\"Created\":\"0\"");
    }

    @Test
    void otherKafkaErrorFailsTheResourceButStillResponds() {
        Admin admin = mock(Admin.class);
        when(admin.createTopics(anyCollection())).thenAnswer(inv ->
                resultFor(inv.<List<NewTopic>>getArgument(0).get(0).name(), new IllegalStateException("boom")));
        new TopicsHandler(() -> admin, SPECS, responder).handleRequest(event("Create", Map.of()), context(60_000));
        assertThat(responses).hasSize(1);
        assertThat(responses.get(0)).contains("FAILED").contains("boom");
    }

    @Test
    void deleteDoesNothing() {
        Admin admin = mock(Admin.class);
        new TopicsHandler(() -> admin, SPECS, responder).handleRequest(event("Delete", Map.of()), context(60_000));
        Mockito.verifyNoInteractions(admin);
        assertThat(responses.get(0)).contains("SUCCESS");
    }

    @Test
    void parsesBundledTopicsFile() {
        List<TopicsHandler.TopicSpec> specs = TopicsHandler.loadBundledSpecs();
        assertThat(specs).extracting(TopicsHandler.TopicSpec::name)
                .contains("sf.change-events.raw", "erp.public.orders", "integration.dlq");
        assertThatThrownBy(() -> TopicsHandler.parse(List.of("only-name 3")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(Optional.of(TopicsHandler.parse(List.of("# c", "", "t 2 5"))).get()).hasSize(1);
    }

    @SuppressWarnings("unused")
    private static KafkaFuture<Void> unused() {
        return KafkaFuture.completedFuture(null);
    }
}
