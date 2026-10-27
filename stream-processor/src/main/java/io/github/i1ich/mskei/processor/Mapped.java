package io.github.i1ich.mskei.processor;

/** One output record of a mapper: the Kafka key (String) and the Avro value. */
public record Mapped<V>(String key, V value) {
}
