package io.github.i1ich.mskei.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.i1ich.mskei.common.DeadLetters;
import io.github.i1ich.mskei.common.EventHeaders;
import io.github.i1ich.mskei.common.Topics;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler.DeserializationHandlerResponse;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.junit.jupiter.api.Test;

class DlqDeserializationExceptionHandlerTest {
    private final ErrorHandlerContext context = mock(ErrorHandlerContext.class);
    private final ConsumerRecord<byte[], byte[]> record =
            new ConsumerRecord<>("some.topic", 2, 41L, new byte[] {1}, new byte[] {9, 9});

    @Test
    void writesRawRecordToDlqAndContinues() {
        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        var handler = new DlqDeserializationExceptionHandler(() -> producer);
        handler.configure(Map.of("bootstrap.servers", "x:1"));

        var response = handler.handle(context, record, new SerializationException("boom"));

        assertThat(response).isEqualTo(DeserializationHandlerResponse.CONTINUE);
        assertThat(producer.history()).hasSize(1);
        ProducerRecord<byte[], byte[]> sent = producer.history().get(0);
        assertThat(sent.topic()).isEqualTo(Topics.DLQ);
        assertThat(sent.key()).containsExactly(1);
        assertThat(sent.value()).containsExactly(9, 9);
        assertThat(DeadLetters.header(sent.headers(), EventHeaders.SOURCE_TOPIC)).isEqualTo("some.topic");
        assertThat(DeadLetters.header(sent.headers(), EventHeaders.SOURCE_PARTITION)).isEqualTo("2");
        assertThat(DeadLetters.header(sent.headers(), EventHeaders.SOURCE_OFFSET)).isEqualTo("41");
        assertThat(DeadLetters.header(sent.headers(), EventHeaders.ERROR_MESSAGE)).isEqualTo("boom");
        assertThat(DeadLetters.header(sent.headers(), EventHeaders.ATTEMPTS)).isEqualTo("1");
    }

    @Test
    void reusesTheSameProducerForLaterRecords() {
        int[] created = {0};
        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        var handler = new DlqDeserializationExceptionHandler(() -> {
            created[0]++;
            return producer;
        });

        handler.handle(context, record, new RuntimeException("1"));
        handler.handle(context, record, new RuntimeException("2"));

        assertThat(created[0]).isEqualTo(1);
        assertThat(producer.history()).hasSize(2);
    }

    @SuppressWarnings("unchecked")
    @Test
    void failsWhenTheDlqWriteFails() {
        Producer<byte[], byte[]> producer = mock(Producer.class);
        when(producer.send(any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));
        var handler = new DlqDeserializationExceptionHandler(() -> producer);

        var response = handler.handle(context, record, new RuntimeException("x"));

        // Never skip a record we could not park: stop the thread instead.
        assertThat(response).isEqualTo(DeserializationHandlerResponse.FAIL);
    }

    @Test
    void failsWhenProducerCannotBeCreated() {
        var handler = new DlqDeserializationExceptionHandler(() -> {
            throw new IllegalStateException("no producer");
        });

        assertThat(handler.handle(context, record, new RuntimeException("x")))
                .isEqualTo(DeserializationHandlerResponse.FAIL);
    }

    @SuppressWarnings("unchecked")
    @Test
    void failsAndKeepsInterruptFlagWhenInterrupted() throws Exception {
        Producer<byte[], byte[]> producer = mock(Producer.class);
        CompletableFuture<org.apache.kafka.clients.producer.RecordMetadata> never = new CompletableFuture<>();
        when(producer.send(any())).thenReturn(never);
        var handler = new DlqDeserializationExceptionHandler(() -> producer);

        Thread.currentThread().interrupt();
        var response = handler.handle(context, record, new RuntimeException("x"));

        assertThat(response).isEqualTo(DeserializationHandlerResponse.FAIL);
        assertThat(Thread.interrupted()).isTrue(); // also clears the flag for other tests
    }
}
