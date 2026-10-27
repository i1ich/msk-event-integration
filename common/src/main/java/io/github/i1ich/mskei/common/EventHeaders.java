package io.github.i1ich.mskei.common;

/** Kafka record header names used across the platform. */
public final class EventHeaders {
    // Set by ingress-lambda on sf.change-events.raw
    public static final String EVENT_ID = "event-id";
    public static final String RECEIVED_AT = "received-at";
    public static final String SOURCE = "source";
    public static final String TRANSACTION_KEY = "transaction-key";

    // Set on integration.dlq
    public static final String ERROR_CLASS = "error-class";
    public static final String ERROR_MESSAGE = "error-message";
    public static final String SOURCE_TOPIC = "source-topic";
    public static final String SOURCE_PARTITION = "source-partition";
    public static final String SOURCE_OFFSET = "source-offset";
    public static final String FAILED_AT = "failed-at";
    public static final String ATTEMPTS = "attempts";

    // Retry and replay
    public static final String NOT_BEFORE = "not-before";
    public static final String REPLAYED_AT = "replayed-at";

    private EventHeaders() {
    }
}
