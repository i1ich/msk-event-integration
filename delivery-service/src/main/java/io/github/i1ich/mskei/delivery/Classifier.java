package io.github.i1ich.mskei.delivery;

import java.io.IOException;

/** Pure functions that decide what a failed (or successful) HTTP call means. No I/O, easy to test. */
public final class Classifier {
    private Classifier() {
    }

    public static Outcome classify(int status) {
        if (status >= 200 && status < 300) {
            return Outcome.SUCCESS;
        }
        // 429 = "slow down", 5xx = downstream is broken right now: both may heal. Other 4xx mean
        // "your request is wrong", and sending the same bytes again will not change that.
        if (status == 429 || status >= 500) {
            return Outcome.RETRYABLE;
        }
        return Outcome.PERMANENT;
    }

    public static Outcome classify(Throwable error) {
        if (error instanceof DownstreamException d) {
            return classify(d.statusCode());
        }
        // Timeouts (HttpTimeoutException) and connection problems are all IOExceptions.
        if (error instanceof IOException) {
            return Outcome.RETRYABLE;
        }
        // Anything else is a bug or bad data on our side; retrying will not fix it.
        return Outcome.PERMANENT;
    }
}
