package io.github.i1ich.mskei.replay;

import static io.github.i1ich.mskei.replay.DlqFixture.dlq;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class ReplayCliTest {
    private static final Map<String, String> ENV = Map.of("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private final StringWriter out = new StringWriter();
    private final StringWriter err = new StringWriter();
    private final MockProducer<byte[], byte[]> producer =
            new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());

    private int run(Map<String, String> env, String... args) {
        List<ConsumerRecord<byte[], byte[]>> records =
                List.of(dlq(0, "k", "v", "integration.account.v1", "x.Err", "2026-01-01T00:00:00Z"));
        ReplayCli.ClientFactory factory = new ReplayCli.ClientFactory() {
            @Override
            public Consumer<byte[], byte[]> consumer(Properties connection) {
                return DlqFixture.consumer(records.size(), records);
            }

            @Override
            public Producer<byte[], byte[]> producer(Properties connection) {
                return producer;
            }
        };
        CommandLine cl = ReplayCli.commandLine(new ReplayCli(factory, env));
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(err));
        return cl.execute(args);
    }

    @Test
    void replayEndToEndPrintsSummary() {
        int code = run(ENV, "--rate", "1000", "--error-class", "Err");
        assertThat(code).isZero();
        assertThat(producer.history()).hasSize(1);
        assertThat(out.toString())
                .contains("Summary: scanned=1 matched=1 replayed=1 skipped(no source-topic)=0 failed=0");
    }

    @Test
    void dryRunPublishesNothing() {
        int code = run(ENV, "--dry-run");
        assertThat(code).isZero();
        assertThat(producer.history()).isEmpty();
        assertThat(out.toString()).contains("would replay", "dry-run", "matched=1 replayed=0");
    }

    @Test
    void rateZeroOrNegativeIsUsageError() {
        assertThat(run(ENV, "--rate", "0")).isEqualTo(2);
        assertThat(err.toString()).contains("--rate must be > 0");
        assertThat(run(ENV, "--rate", "-5")).isEqualTo(2);
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void otherInvalidArguments() {
        assertThat(run(ENV, "--max", "0")).isEqualTo(2);
        assertThat(run(ENV, "--from", "not-a-date")).isEqualTo(2);
        assertThat(run(ENV, "--from", "2026-02-01T00:00:00Z", "--to", "2026-01-01T00:00:00Z")).isEqualTo(2);
        assertThat(run(ENV, "--rate", "abc")).isEqualTo(2);
        assertThat(run(ENV, "--no-such-option")).isEqualTo(2);
    }

    @Test
    void validTimeRangeIsAccepted() {
        assertThat(run(ENV, "--dry-run", "--from", "2025-12-31T00:00:00Z", "--to", "2026-02-01T00:00:00Z",
                "--source-topic", "integration.account.v1", "--max", "5")).isZero();
        assertThat(out.toString()).contains("matched=1");
    }

    @Test
    void missingBootstrapServersIsRuntimeError() {
        assertThat(run(Map.of())).isEqualTo(1);
        assertThat(err.toString()).contains("KAFKA_BOOTSTRAP_SERVERS");
    }

    @Test
    void bootstrapOptionOverridesEnvironment() {
        assertThat(ReplayCli.connectionProperties(ENV, "other:9093").get("bootstrap.servers"))
                .isEqualTo("other:9093");
        assertThat(ReplayCli.connectionProperties(ENV, null).get("bootstrap.servers")).isEqualTo("localhost:9092");
        // works even when the env var is missing
        assertThat(run(Map.of(), "--bootstrap-servers", "x:1", "--dry-run")).isZero();
    }

    @Test
    void realKafkaClientsGetSafeSettings() {
        Properties conn = ReplayCli.connectionProperties(Map.of("KAFKA_BOOTSTRAP_SERVERS", "localhost:1"), null);
        // Creating clients does not connect; it only validates config.
        try (Consumer<byte[], byte[]> c = ReplayCli.kafkaClients().consumer(conn);
                Producer<byte[], byte[]> p = ReplayCli.kafkaClients().producer(conn)) {
            assertThat(c).isNotNull();
            assertThat(p).isNotNull();
        }
    }
}
