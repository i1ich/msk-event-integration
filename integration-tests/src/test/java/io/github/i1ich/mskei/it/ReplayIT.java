package io.github.i1ich.mskei.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.i1ich.mskei.common.EventHeaders;
import io.github.i1ich.mskei.common.Topics;
import io.github.i1ich.mskei.it.support.Events;
import io.github.i1ich.mskei.it.support.TopicTail;
import io.github.i1ich.mskei.replay.ReplayCli;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/** DLQ -> replay-cli -> source topic. */
class ReplayIT extends AbstractIT {

    @Test
    void dryRunPublishesNothingAndRealRunRepublishesWithReplayedAtAndNoErrorHeaders() {
        Instant before = Instant.now();
        String id = "006" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase() + "AAA";
        String body = Events.salesforce("Opportunity", "CREATE", id, Map.of("Name", "Replay me"));
        assertThat(ENV.ingress().handle(body, Map.of()).status()).isEqualTo(202);

        try (TopicTail dlq = ENV.tail(Topics.DLQ); TopicTail raw = ENV.tail(Topics.SF_CHANGE_EVENTS_RAW)) {
            ConsumerRecord<byte[], byte[]> dead = dlq.awaitOne(TopicTail.keyIs(id));
            Instant failedAt = Instant.parse(TopicTail.header(dead, EventHeaders.FAILED_AT));
            String originalEventId = TopicTail.header(dead, EventHeaders.EVENT_ID);
            // Narrow window: only this test's record failed in [before, failedAt + 1 ms).
            String[] filters = {
                "--bootstrap-servers", ENV.bootstrapServers(),
                "--error-class", "UnmappedEntityException",
                "--source-topic", Topics.SF_CHANGE_EVENTS_RAW,
                "--from", before.toString(),
                "--to", failedAt.plusMillis(1).toString(),
                "--rate", "50"};

            // 1. dry run: reports the record, publishes nothing
            Result dry = run(concat(filters, "--dry-run"));
            assertThat(dry.exitCode).isZero();
            assertThat(dry.out).contains("would replay").contains(originalEventId).contains("matched=1");
            raw.awaitStableCount(TopicTail.keyIs(id), 1, Duration.ofSeconds(3)); // still only the original

            // 2. real run
            Result real = run(filters);
            assertThat(real.exitCode).isZero();
            assertThat(real.out).contains("matched=1").contains("replayed=1");

            List<ConsumerRecord<byte[], byte[]>> records = raw.await(TopicTail.keyIs(id), 2);
            ConsumerRecord<byte[], byte[]> replayed = records.get(1);
            assertThat(TopicTail.header(replayed, EventHeaders.REPLAYED_AT)).isNotBlank();
            assertThat(TopicTail.header(replayed, EventHeaders.EVENT_ID)).isEqualTo(originalEventId);
            assertThat(new String(replayed.value(), StandardCharsets.UTF_8)).isEqualTo(body);
            for (String errorHeader : List.of(EventHeaders.ERROR_CLASS, EventHeaders.ERROR_MESSAGE,
                    EventHeaders.SOURCE_TOPIC, EventHeaders.SOURCE_PARTITION, EventHeaders.SOURCE_OFFSET,
                    EventHeaders.FAILED_AT, EventHeaders.ATTEMPTS)) {
                assertThat(replayed.headers().lastHeader(errorHeader)).as(errorHeader).isNull();
            }
            // The replayed record is processed again (and fails again: still an unknown entity).
            dlq.await(TopicTail.keyIs(id), 2);
        }
    }

    private record Result(int exitCode, String out) {
    }

    private static Result run(String... args) {
        StringWriter out = new StringWriter();
        CommandLine cmd = new CommandLine(new ReplayCli());
        cmd.setOut(new PrintWriter(out, true));
        cmd.setErr(new PrintWriter(out, true));
        int exit = cmd.execute(args);
        return new Result(exit, out.toString());
    }

    private static String[] concat(String[] a, String... b) {
        String[] r = new String[a.length + b.length];
        System.arraycopy(a, 0, r, 0, a.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }
}
