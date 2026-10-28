package io.github.i1ich.mskei.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.junit.jupiter.api.Test;

class GeneratedSchemasTest {

    private static String canonical(String schemaJson) {
        return SchemaNormalization.toParsingForm(new Schema.Parser().parse(schemaJson));
    }

    private static String file(String path) throws IOException {
        return Files.readString(Path.of("..", "schemas", path));
    }

    @Test
    void generatedAccountSchemaIsTheLatestAvscPlusJavaStringHints() throws IOException {
        String generated = GeneratedSchemas.definition(Topics.ACCOUNT_V1);
        // Same schema for Avro (canonical form ignores doc/props) ...
        assertThat(canonical(generated)).isEqualTo(canonical(file("account/AccountEvent.v2.avsc")));
        // ... but a different text, which is why it has to be registered as-is for the Glue lookup.
        assertThat(generated).contains("avro.java.string");
    }

    @Test
    void generatedOrderSchemaIsTheLatestAvsc() throws IOException {
        assertThat(canonical(GeneratedSchemas.definition(Topics.ORDER_V1)))
                .isEqualTo(canonical(file("order/OrderEvent.v1.avsc")));
    }

    @Test
    void unknownTopicIsRejected() {
        assertThatThrownBy(() -> GeneratedSchemas.definition("nope")).isInstanceOf(IllegalArgumentException.class);
    }
}
