package io.github.i1ich.mskei.common;

import io.github.i1ich.mskei.schema.AccountEvent;
import io.github.i1ich.mskei.schema.OrderEvent;
import java.util.Map;
import org.apache.avro.Schema;

/**
 * Prints the schema text of the Avro-generated classes, per topic.
 *
 * <p>Why: the generated classes carry {@code "avro.java.string":"String"} (from {@code stringType=String}), so their
 * schema text differs from the .avsc files in schemas/. The Glue serializer looks a schema up by its exact
 * definition and has no "use latest version" option, so with auto-registration off the registered latest version must
 * be this text. It is semantically the same schema as the latest .avsc (same canonical form, see the test).
 *
 * <p>Usage (by scripts/register-schemas-glue.sh): {@code java -cp stream-processor/target/stream-processor.jar
 * io.github.i1ich.mskei.common.GeneratedSchemas integration.account.v1}
 */
public final class GeneratedSchemas {
    static final Map<String, Schema> BY_TOPIC = Map.of(
            Topics.ACCOUNT_V1, AccountEvent.getClassSchema(),
            Topics.ORDER_V1, OrderEvent.getClassSchema());

    private GeneratedSchemas() {
    }

    public static String definition(String topic) {
        Schema schema = BY_TOPIC.get(topic);
        if (schema == null) {
            throw new IllegalArgumentException("No generated schema for topic " + topic + "; known: " + BY_TOPIC.keySet());
        }
        return schema.toString();
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("usage: GeneratedSchemas <topic>");
            System.exit(2);
        }
        System.out.println(definition(args[0]));
    }
}
