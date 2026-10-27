package io.github.i1ich.mskei.delivery;

/** What a delivery attempt means for the record. */
public enum Outcome {
    /** Downstream accepted the event. */
    SUCCESS,
    /** Might work later (429, 5xx, timeout, network error): walk the retry ladder. */
    RETRYABLE,
    /** Will never work as is (other 4xx, bad payload): straight to the DLQ. */
    PERMANENT
}
