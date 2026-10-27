package io.github.i1ich.mskei.delivery;

/** Downstream answered with a non-2xx status. Its text ends up in the DLQ "error-message" header. */
public class DownstreamException extends RuntimeException {
    private final int statusCode;

    public DownstreamException(int statusCode) {
        super("Downstream returned HTTP " + statusCode);
        this.statusCode = statusCode;
    }

    public int statusCode() {
        return statusCode;
    }
}
