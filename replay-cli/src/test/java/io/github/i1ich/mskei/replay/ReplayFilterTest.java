package io.github.i1ich.mskei.replay;

import static io.github.i1ich.mskei.replay.DlqFixture.dlq;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.i1ich.mskei.replay.ReplayFilter.Verdict;
import java.time.Instant;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;

class ReplayFilterTest {
    private static final String ERR = "io.github.i1ich.mskei.stream.UnmappedEntityException";

    private static Headers h(String topic, String err, String failedAt) {
        return dlq(0, "k", "v", topic, err, failedAt).headers();
    }

    @Test
    void emptyFilterMatchesEverythingWithSourceTopic() {
        ReplayFilter f = new ReplayFilter(null, null, null, null);
        assertThat(f.evaluate(h("t", ERR, "2026-01-01T00:00:00Z"))).isEqualTo(Verdict.MATCH);
        assertThat(f.evaluate(h("t", null, null))).isEqualTo(Verdict.MATCH);
    }

    @Test
    void sourceTopicIsExactMatch() {
        ReplayFilter f = new ReplayFilter("integration.account.v1", null, null, null);
        assertThat(f.evaluate(h("integration.account.v1", ERR, null))).isEqualTo(Verdict.MATCH);
        assertThat(f.evaluate(h("integration.account.v1.x", ERR, null))).isEqualTo(Verdict.NO_MATCH);
    }

    @Test
    void errorClassMatchesFullOrSimpleName() {
        assertThat(new ReplayFilter(null, ERR, null, null).evaluate(h("t", ERR, null))).isEqualTo(Verdict.MATCH);
        ReplayFilter simple = new ReplayFilter(null, "UnmappedEntityException", null, null);
        assertThat(simple.evaluate(h("t", ERR, null))).isEqualTo(Verdict.MATCH);
        assertThat(simple.evaluate(h("t", "UnmappedEntityException", null))).isEqualTo(Verdict.MATCH);
        assertThat(simple.evaluate(h("t", "java.lang.IllegalStateException", null))).isEqualTo(Verdict.NO_MATCH);
        assertThat(simple.evaluate(h("t", null, null))).isEqualTo(Verdict.NO_MATCH);
        // partial names must not match
        assertThat(new ReplayFilter(null, "Exception", null, null).evaluate(h("t", ERR, null)))
                .isEqualTo(Verdict.NO_MATCH);
    }

    @Test
    void timeRangeIsInclusiveFromAndExclusiveTo() {
        Instant from = Instant.parse("2026-01-01T10:00:00Z");
        Instant to = Instant.parse("2026-01-01T11:00:00Z");
        ReplayFilter f = new ReplayFilter(null, null, from, to);
        assertThat(f.evaluate(h("t", ERR, "2026-01-01T10:00:00Z"))).isEqualTo(Verdict.MATCH);
        assertThat(f.evaluate(h("t", ERR, "2026-01-01T10:59:59Z"))).isEqualTo(Verdict.MATCH);
        assertThat(f.evaluate(h("t", ERR, "2026-01-01T11:00:00Z"))).isEqualTo(Verdict.NO_MATCH);
        assertThat(f.evaluate(h("t", ERR, "2026-01-01T09:59:59Z"))).isEqualTo(Verdict.NO_MATCH);
    }

    @Test
    void openEndedRanges() {
        Instant t = Instant.parse("2026-01-01T10:00:00Z");
        assertThat(new ReplayFilter(null, null, t, null).evaluate(h("x", ERR, "2027-01-01T00:00:00Z")))
                .isEqualTo(Verdict.MATCH);
        assertThat(new ReplayFilter(null, null, null, t).evaluate(h("x", ERR, "2025-01-01T00:00:00Z")))
                .isEqualTo(Verdict.MATCH);
    }

    @Test
    void missingOrBrokenFailedAtDoesNotMatchWhenRangeIsGiven() {
        ReplayFilter f = new ReplayFilter(null, null, Instant.EPOCH, null);
        assertThat(f.evaluate(h("t", ERR, null))).isEqualTo(Verdict.NO_MATCH);
        assertThat(f.evaluate(h("t", ERR, "yesterday"))).isEqualTo(Verdict.NO_MATCH);
    }

    @Test
    void combinedFiltersAllHaveToMatch() {
        ReplayFilter f = new ReplayFilter("a", "UnmappedEntityException", Instant.EPOCH, null);
        assertThat(f.evaluate(h("a", ERR, "2026-01-01T00:00:00Z"))).isEqualTo(Verdict.MATCH);
        assertThat(f.evaluate(h("b", ERR, "2026-01-01T00:00:00Z"))).isEqualTo(Verdict.NO_MATCH);
        assertThat(f.evaluate(h("a", "Other", "2026-01-01T00:00:00Z"))).isEqualTo(Verdict.NO_MATCH);
    }

    @Test
    void recordWithoutSourceTopicIsReportedSeparately() {
        ReplayFilter f = new ReplayFilter("a", null, null, null);
        assertThat(f.evaluate(h(null, ERR, null))).isEqualTo(Verdict.NO_SOURCE_TOPIC);
        // but if it fails another filter it is simply not interesting
        assertThat(new ReplayFilter(null, "Nope", null, null).evaluate(h(null, ERR, null)))
                .isEqualTo(Verdict.NO_MATCH);
    }
}
