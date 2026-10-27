package io.github.i1ich.mskei.common;

/** Topic names. The single source of truth for code; infra scripts mirror these. */
public final class Topics {
    public static final String SF_CHANGE_EVENTS_RAW = "sf.change-events.raw";
    public static final String ERP_ORDERS_CDC = "erp.public.orders";
    public static final String ACCOUNT_V1 = "integration.account.v1";
    public static final String ORDER_V1 = "integration.order.v1";
    public static final String RETRY_1M = "integration.retry.1m";
    public static final String RETRY_10M = "integration.retry.10m";
    public static final String DLQ = "integration.dlq";

    private Topics() {
    }
}
