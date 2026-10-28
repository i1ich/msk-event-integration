package io.github.i1ich.mskei.replay;

import io.github.i1ich.mskei.common.KafkaClientConfig;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

/** Rate-limited DLQ replay tool. See replay-cli/README.md and docs/runbooks/dlq-replay.md. */
@Command(name = "replay-cli", mixinStandardHelpOptions = true,
        description = "Republishes records from integration.dlq to their source topic.")
public final class ReplayCli implements Callable<Integer> {

    /** Creates the Kafka clients. A seam so tests can plug in MockConsumer/MockProducer. */
    interface ClientFactory {
        Consumer<byte[], byte[]> consumer(Properties connection);

        Producer<byte[], byte[]> producer(Properties connection);
    }

    private static final ClientFactory KAFKA = new ClientFactory() {
        @Override
        public Consumer<byte[], byte[]> consumer(Properties connection) {
            Properties p = new Properties();
            p.putAll(connection);
            p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            p.put("enable.auto.commit", "false"); // no group.id on purpose, see DlqReplayer
            // Part of the DLQ is written inside stream-processor transactions; never replay an aborted record.
            p.put("isolation.level", "read_committed");
            return new KafkaConsumer<>(p);
        }

        @Override
        public Producer<byte[], byte[]> producer(Properties connection) {
            Properties p = new Properties();
            p.putAll(connection);
            p.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            p.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            p.put("acks", "all");
            p.put("enable.idempotence", "true"); // retries inside the producer never create duplicates
            return new KafkaProducer<>(p);
        }
    };

    /** The real Kafka clients (package-private so a test can check their config). */
    static ClientFactory kafkaClients() {
        return KAFKA;
    }

    @Spec
    CommandSpec spec;

    @Option(names = "--source-topic", description = "Only records whose source-topic header equals this.")
    String sourceTopic;

    @Option(names = "--error-class", description = "Only records with this error-class (full or simple name).")
    String errorClass;

    @Option(names = "--from", converter = InstantConverter.class,
            description = "Only records with failed-at >= this ISO-8601 instant.")
    Instant from;

    @Option(names = "--to", converter = InstantConverter.class,
            description = "Only records with failed-at < this ISO-8601 instant.")
    Instant to;

    @Option(names = "--rate", defaultValue = "10", description = "Messages per second (> 0). Default: ${DEFAULT-VALUE}.")
    double rate;

    @Option(names = "--max", description = "Stop after this many matching records.")
    Long max;

    @Option(names = "--dry-run", description = "Print what would be replayed; publish nothing.")
    boolean dryRun;

    @Option(names = "--bootstrap-servers", description = "Overrides env KAFKA_BOOTSTRAP_SERVERS.")
    String bootstrapServers;

    private final ClientFactory clients;
    private final Map<String, String> env;

    public ReplayCli() {
        this(KAFKA, System.getenv());
    }

    ReplayCli(ClientFactory clients, Map<String, String> env) {
        this.clients = clients;
        this.env = env;
    }

    /** Parses "2026-01-31T10:00:00Z" style text. */
    static final class InstantConverter implements CommandLine.ITypeConverter<Instant> {
        @Override
        public Instant convert(String value) {
            try {
                return Instant.parse(value);
            } catch (DateTimeParseException e) {
                throw new CommandLine.TypeConversionException(
                        "'" + value + "' is not an ISO-8601 instant, e.g. 2026-01-31T10:00:00Z");
            }
        }
    }

    @Override
    public Integer call() throws Exception {
        validate();
        Properties connection = connectionProperties(env, bootstrapServers);
        ReplayFilter filter = new ReplayFilter(sourceTopic, errorClass, from, to);
        PrintWriter out = spec.commandLine().getOut();

        // DlqReplayer prints to a PrintStream; collect it and forward to picocli's writer
        PrintStream stream = new PrintStream(new WriterOutputStream(out), true);
        DlqReplayer.Summary s;
        try (Consumer<byte[], byte[]> consumer = clients.consumer(connection);
                Producer<byte[], byte[]> producer = clients.producer(connection)) {
            s = new DlqReplayer(consumer, producer, filter, RateLimiter.system(rate), max, dryRun, stream).run();
        }
        stream.flush();
        out.printf("Summary%s: scanned=%d matched=%d replayed=%d skipped(no source-topic)=%d failed=%d%n",
                dryRun ? " (dry-run, nothing published)" : "", s.scanned(), s.matched(), s.replayed(),
                s.skipped(), s.failed());
        out.flush();
        return s.failed() > 0 ? 1 : 0;
    }

    private void validate() {
        CommandLine cmd = spec.commandLine();
        if (!(rate > 0)) {
            throw new ParameterException(cmd, "--rate must be > 0, got " + rate);
        }
        if (max != null && max <= 0) {
            throw new ParameterException(cmd, "--max must be > 0, got " + max);
        }
        if (from != null && to != null && !from.isBefore(to)) {
            throw new ParameterException(cmd, "--from must be before --to");
        }
    }

    /** Env-based connection settings, with an optional bootstrap override from the command line. */
    static Properties connectionProperties(Map<String, String> env, String bootstrapOverride) {
        Map<String, String> merged = new HashMap<>(env);
        if (bootstrapOverride != null && !bootstrapOverride.isBlank()) {
            merged.put(KafkaClientConfig.ENV_BOOTSTRAP, bootstrapOverride);
        }
        return KafkaClientConfig.from(merged);
    }

    /** Builds the picocli command line. Exit codes: 0 ok, 1 runtime error or failed sends, 2 bad arguments. */
    static CommandLine commandLine(ReplayCli cli) {
        CommandLine cl = new CommandLine(cli);
        cl.setExecutionExceptionHandler((ex, c, parseResult) -> {
            c.getErr().println("Error: " + ex.getMessage());
            c.getErr().flush();
            return ex instanceof ParameterException ? 2 : 1;
        });
        return cl;
    }

    public static void main(String[] args) {
        System.exit(commandLine(new ReplayCli()).execute(args));
    }
}
