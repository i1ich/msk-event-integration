package io.github.i1ich.mskei.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.http.HttpTimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ClassifierTest {
    @ParameterizedTest
    @CsvSource({
        "200,SUCCESS", "201,SUCCESS", "204,SUCCESS",
        "400,PERMANENT", "404,PERMANENT", "409,PERMANENT", "422,PERMANENT",
        "429,RETRYABLE", "500,RETRYABLE", "503,RETRYABLE"
    })
    void statusTable(int status, Outcome expected) {
        assertThat(Classifier.classify(status)).isEqualTo(expected);
    }

    @Test
    void downstreamExceptionUsesItsStatus() {
        assertThat(Classifier.classify(new DownstreamException(503))).isEqualTo(Outcome.RETRYABLE);
        assertThat(Classifier.classify(new DownstreamException(400))).isEqualTo(Outcome.PERMANENT);
        assertThat(new DownstreamException(503).statusCode()).isEqualTo(503);
    }

    @Test
    void ioProblemsAreRetryable() {
        assertThat(Classifier.classify(new IOException("connection reset"))).isEqualTo(Outcome.RETRYABLE);
        assertThat(Classifier.classify(new HttpTimeoutException("timed out"))).isEqualTo(Outcome.RETRYABLE);
    }

    @Test
    void unexpectedErrorsArePermanent() {
        assertThat(Classifier.classify(new IllegalArgumentException("bad"))).isEqualTo(Outcome.PERMANENT);
    }
}
