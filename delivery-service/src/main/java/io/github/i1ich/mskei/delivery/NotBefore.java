package io.github.i1ich.mskei.delivery;

import io.github.i1ich.mskei.common.DeadLetters;
import io.github.i1ich.mskei.common.EventHeaders;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import org.apache.kafka.common.header.Headers;

/** Reads the {@code not-before} header of a retry record. */
public final class NotBefore {
    private NotBefore() {
    }

    /** The instant before which the record must not be delivered, or null if absent / unreadable. */
    public static Instant parse(Headers headers) {
        String text = DeadLetters.header(headers, EventHeaders.NOT_BEFORE);
        if (text == null) {
            return null;
        }
        try {
            return Instant.parse(text);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    /** A record without a usable not-before is due now: better an early retry than a stuck partition. */
    public static boolean isDue(Headers headers, Instant now) {
        Instant notBefore = parse(headers);
        return notBefore == null || !notBefore.isAfter(now);
    }
}
