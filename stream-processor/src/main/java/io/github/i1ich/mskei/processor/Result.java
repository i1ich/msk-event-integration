package io.github.i1ich.mskei.processor;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * What {@link SafeMapProcessor} emits downstream: EITHER a mapped value OR a ready-to-send dead letter.
 * A tiny "either" type lets one processor feed two branches (output topic / DLQ) without throwing.
 */
public record Result<V>(V value, ProducerRecord<byte[], byte[]> deadLetter) {

    public static <V> Result<V> ok(V value) {
        return new Result<>(value, null);
    }

    public static <V> Result<V> failed(ProducerRecord<byte[], byte[]> deadLetter) {
        return new Result<>(null, deadLetter);
    }

    public boolean isFailed() {
        return deadLetter != null;
    }
}
