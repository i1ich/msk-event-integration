package io.github.i1ich.mskei.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

/**
 * Schema evolution rules checked with plain Avro (no Schema Registry). The working directory of
 * surefire is the module directory, so schemas are loaded from {@code ../schemas}.
 *
 * <p>BACKWARD compatibility (the registry mode used by this project) means: a consumer using the NEW
 * schema can read data written with the OLD schema. In Avro terms the new schema is the reader and
 * the old one the writer. What breaks it is a reader field that the writer did not write and that
 * has no default: the reader has no value to fill in. That is why "add a required field without a
 * default" is the canonical BACKWARD break, and it is what the fixture does (adds {@code region}).
 *
 * <p>The spec's example "removing a required field without default" describes the same rule from the
 * other side: it breaks OLD readers of NEW data (FORWARD direction), because the old reader still
 * expects the removed field and has no default. In Avro, removing a field is BACKWARD compatible
 * (the new reader simply skips it), so the fixture adds a field instead of removing one.
 */
class SchemaEvolutionTest {

  private static final File SCHEMAS = new File("../schemas");

  private static Schema load(String relativePath) throws IOException {
    return new Schema.Parser().parse(new File(SCHEMAS, relativePath));
  }

  @Test
  void v2ReaderCanReadV1Data() throws IOException {
    Schema v1 = load("account/AccountEvent.v1.avsc");
    Schema v2 = load("account/AccountEvent.v2.avsc");

    assertThat(SchemaCompatibility.checkReaderWriterCompatibility(v2, v1).getType())
        .isEqualTo(SchemaCompatibilityType.COMPATIBLE);
  }

  @Test
  void v1ReaderCanReadV2Data() throws IOException {
    Schema v1 = load("account/AccountEvent.v1.avsc");
    Schema v2 = load("account/AccountEvent.v2.avsc");

    assertThat(SchemaCompatibility.checkReaderWriterCompatibility(v1, v2).getType())
        .isEqualTo(SchemaCompatibilityType.COMPATIBLE);
  }

  @Test
  void oldConsumerIgnoresIndustryWrittenByNewProducer() throws IOException {
    Schema v1 = load("account/AccountEvent.v1.avsc");
    Schema v2 = load("account/AccountEvent.v2.avsc");

    GenericRecord written = new GenericData.Record(v2);
    written.put("eventId", "e-1");
    written.put("accountId", "a-1");
    written.put("changeType", new GenericData.EnumSymbol(v2.getField("changeType").schema(), "UPDATE"));
    written.put("name", "Acme");
    written.put("commitTimestamp", 1_700_000_000_000L);
    written.put("sourceSystem", "salesforce");
    written.put("industry", "Software");

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(v2).write(written, encoder);
    encoder.flush();

    GenericRecord read =
        new GenericDatumReader<GenericRecord>(v2, v1)
            .read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));

    assertThat(read.getSchema().getField("industry")).isNull();
    assertThat(read.get("accountId").toString()).isEqualTo("a-1");
    assertThat(read.get("name").toString()).isEqualTo("Acme");
  }

  @Test
  void newRequiredFieldWithoutDefaultIsNotBackwardCompatible() throws IOException {
    Schema v2 = load("account/AccountEvent.v2.avsc");
    Schema incompatible = load("test-fixtures/AccountEvent.incompatible.avsc");

    // BACKWARD check: new schema (fixture) reads data written with the latest registered one (v2).
    SchemaCompatibility.SchemaPairCompatibility result =
        SchemaCompatibility.checkReaderWriterCompatibility(incompatible, v2);

    assertThat(result.getType()).isEqualTo(SchemaCompatibilityType.INCOMPATIBLE);
    assertThat(result.getDescription()).contains("region");
  }

  @Test
  void orderSchemaParses() throws IOException {
    assertThat(load("order/OrderEvent.v1.avsc").getName()).isEqualTo("OrderEvent");
  }
}
