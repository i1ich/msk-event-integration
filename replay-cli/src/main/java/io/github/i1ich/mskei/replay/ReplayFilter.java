package io.github.i1ich.mskei.replay;

import io.github.i1ich.mskei.common.DeadLetters;
import io.github.i1ich.mskei.common.EventHeaders;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import org.apache.kafka.common.header.Headers;

/**
 * Which DLQ records to replay. Every null field means "do not filter on this".
 *
 * @param sourceTopic exact match on header {@code source-topic}
 * @param errorClass  exact class name, or simple name (text after the last dot)
 * @param from        inclusive lower bound on {@code failed-at}
 * @param to          exclusive upper bound on {@code failed-at}
 */
public record ReplayFilter(String sourceTopic, String errorClass, Instant from, Instant to) {

    public enum Verdict {
        /** Does not pass the filters: ignore. */
        NO_MATCH,
        /** Passes error-class/time filters but has no source-topic header, so we cannot know where to send it. */
        NO_SOURCE_TOPIC,
        /** Replay it. */
        MATCH
    }

    public Verdict evaluate(Headers headers) {
        if (!errorClassMatches(DeadLetters.header(headers, EventHeaders.ERROR_CLASS))) {
            return Verdict.NO_MATCH;
        }
        if (!timeMatches(DeadLetters.header(headers, EventHeaders.FAILED_AT))) {
            return Verdict.NO_MATCH;
        }
        String topic = DeadLetters.header(headers, EventHeaders.SOURCE_TOPIC);
        if (topic == null || topic.isBlank()) {
            return Verdict.NO_SOURCE_TOPIC;
        }
        if (sourceTopic != null && !sourceTopic.equals(topic)) {
            return Verdict.NO_MATCH;
        }
        return Verdict.MATCH;
    }

    private boolean errorClassMatches(String actual) {
        if (errorClass == null) {
            return true;
        }
        if (actual == null) {
            return false;
        }
        String simpleName = actual.substring(actual.lastIndexOf('.') + 1);
        return actual.equals(errorClass) || simpleName.equals(errorClass);
    }

    private boolean timeMatches(String failedAtText) {
        if (from == null && to == null) {
            return true;
        }
        if (failedAtText == null) {
            return false; // cannot prove it is inside the requested window
        }
        Instant failedAt;
        try {
            failedAt = Instant.parse(failedAtText);
        } catch (DateTimeParseException e) {
            return false;
        }
        boolean afterFrom = from == null || !failedAt.isBefore(from);
        boolean beforeTo = to == null || failedAt.isBefore(to);
        return afterFrom && beforeTo;
    }
}
