package io.github.i1ich.mskei.processor;

import io.github.i1ich.mskei.common.DeadLetters;
import java.util.List;
import org.apache.kafka.common.header.Headers;

/**
 * Turns one raw input record into zero or more canonical events. It may throw ANY exception: the caller
 * ({@link SafeMapProcessor}) turns it into a dead-letter record. An empty list means "skip on purpose".
 */
@FunctionalInterface
public interface RecordMapper<V> {
    List<Mapped<V>> map(byte[] value, Headers headers, DeadLetters.Source source) throws Exception;
}
