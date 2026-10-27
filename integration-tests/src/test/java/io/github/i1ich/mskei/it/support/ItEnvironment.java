package io.github.i1ich.mskei.it.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.github.i1ich.mskei.delivery.DeliveryServiceApp;
import io.github.i1ich.mskei.ingress.EventIngestService;
import io.github.i1ich.mskei.ingress.IngressConfig;
import io.github.i1ich.mskei.ingress.ProducerFactory;
import io.github.i1ich.mskei.processor.StreamProcessorApp;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

/**
 * The shared end-to-end environment, started once per JVM (singleton container pattern) and torn down by a
 * shutdown hook (containers are additionally reaped by Ryuk).
 *
 * <pre>
 *  containers (one Docker network): Kafka, Schema Registry, PostgreSQL (wal_level=logical), Debezium Connect
 *  in this JVM:                     stream-processor, delivery-service, WireMock (downstream), ingress service
 * </pre>
 *
 * Mirrors docker-compose.yml: same images, same topics, same schemas, same connector config, same WireMock mappings.
 */
public final class ItEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(ItEnvironment.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    /** Failsafe runs with the module directory as working directory; the repo root is one level up. */
    private static final Path REPO = Path.of("..").toAbsolutePath().normalize();

    private static ItEnvironment instance;

    private final Network network = Network.newNetwork();
    private KafkaBroker kafka;
    private GenericContainer<?> schemaRegistry;
    private PostgreSQLContainer<?> postgres;
    private GenericContainer<?> connect;
    private WireMockServer wireMock;
    private KafkaStreams streams;
    private DeliveryServiceApp.Running delivery;
    private Producer<String, String> ingressProducer;
    private EventIngestService ingress;

    private ItEnvironment() {
    }

    public static synchronized ItEnvironment get() {
        if (instance == null) {
            Awaitility.setDefaultTimeout(Duration.ofSeconds(90));
            Awaitility.setDefaultPollInterval(Duration.ofMillis(250));
            ItEnvironment env = new ItEnvironment();
            Runtime.getRuntime().addShutdownHook(new Thread(env::stop, "it-environment-shutdown"));
            env.start();
            instance = env;
        }
        return instance;
    }

    // ---- accessors -------------------------------------------------------------------------------------------

    public String bootstrapServers() {
        return kafka.getBootstrapServers();
    }

    public String schemaRegistryUrl() {
        return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
    }

    public WireMockServer wireMock() {
        return wireMock;
    }

    public EventIngestService ingress() {
        return ingress;
    }

    public Connection jdbc() throws SQLException {
        return DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    /** Plain-bytes consumer over a topic, reading from the beginning. */
    public TopicTail tail(String topic) {
        return new TopicTail(bootstrapServers(), topic);
    }

    /** Producer of raw bytes, for tests that write straight to a topic. */
    public Producer<byte[], byte[]> bytesProducer() {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrapServers());
        p.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        p.put("acks", "all");
        return new KafkaProducer<>(p);
    }

    /** Avro deserializer with the specific reader: returns AccountEvent / OrderEvent instances. */
    public KafkaAvroDeserializer avroDeserializer() {
        KafkaAvroDeserializer d = new KafkaAvroDeserializer();
        d.configure(Map.of("schema.registry.url", schemaRegistryUrl(), "specific.avro.reader", true), false);
        return d;
    }

    // ---- start-up --------------------------------------------------------------------------------------------

    private void start() {
        long t0 = System.nanoTime();
        kafka = new KafkaBroker("confluentinc/cp-kafka:7.9.0");
        kafka.withNetwork(network)
                .withNetworkAliases("kafka")
                // KafkaBroker also exposes the in-network listener kafka:19092 (Schema Registry, Connect);
                // tests use the mapped host port.
                // As in docker-compose: topics are created explicitly, never implicitly.
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                // exactly_once_v2 on a single broker needs the transaction log to be satisfiable with 1 replica.
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");

        schemaRegistry = new GenericContainer<>("confluentinc/cp-schema-registry:7.9.0")
                .withNetwork(network)
                .withNetworkAliases("schema-registry")
                .withExposedPorts(8081)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .withEnv("SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL", "backward")
                .waitingFor(Wait.forHttp("/subjects").forPort(8081).forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(3)))
                .dependsOn(kafka);

        postgres = new PostgreSQLContainer<>("postgres:16")
                .withDatabaseName("erp").withUsername("erp").withPassword("erp")
                .withNetwork(network)
                .withNetworkAliases("postgres")
                // Debezium reads the WAL through logical decoding (pgoutput).
                .withCommand("postgres", "-c", "wal_level=logical")
                // Same init script as docker-compose: table, REPLICA IDENTITY FULL, publication, seed rows.
                .withCopyFileToContainer(
                        MountableFile.forHostPath(REPO.resolve("infra/postgres/init/01-schema.sql")),
                        "/docker-entrypoint-initdb.d/01-schema.sql");

        // Built from infra/connect/Dockerfile (cp-kafka-connect + Debezium via confluent-hub); Docker caches it.
        connect = new GenericContainer<>(new ImageFromDockerfile("msk-ei/connect-debezium-it:latest", false)
                .withFileFromPath(".", REPO.resolve("infra/connect")))
                .withNetwork(network)
                .withNetworkAliases("connect")
                .withExposedPorts(8083)
                .withEnv("CONNECT_BOOTSTRAP_SERVERS", "kafka:19092")
                .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "connect")
                .withEnv("CONNECT_REST_PORT", "8083")
                .withEnv("CONNECT_GROUP_ID", "connect-cluster")
                .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "_connect-configs")
                .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "_connect-offsets")
                .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "_connect-status")
                .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
                .withEnv("CONNECT_KEY_CONVERTER", "io.confluent.connect.avro.AvroConverter")
                .withEnv("CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
                .withEnv("CONNECT_VALUE_CONVERTER", "io.confluent.connect.avro.AvroConverter")
                .withEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
                .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/usr/share/confluent-hub-components")
                // Worker start-up is slow (plugin scan): 60-120 s is normal.
                .waitingFor(Wait.forHttp("/connectors").forPort(8083).forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(6)))
                .dependsOn(kafka, schemaRegistry, postgres);

        Startables.deepStart(List.of(kafka, schemaRegistry, postgres, connect)).join();
        LOG.info("Containers up after {} s", secondsSince(t0));

        createTopics();
        registerSchemas();
        startWireMock();
        startServices();
        registerConnector();
        LOG.info("Environment ready after {} s", secondsSince(t0));
    }

    private void createTopics() {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrapServers()))) {
            List<NewTopic> topics = new ArrayList<>();
            for (String line : Files.readAllLines(REPO.resolve("scripts/topics.txt"))) {
                String l = line.trim();
                if (l.isEmpty() || l.startsWith("#")) {
                    continue;
                }
                String[] f = l.split("\\s+");
                topics.add(new NewTopic(f[0], Integer.parseInt(f[1]), (short) 1)
                        .configs(Map.of("retention.ms", f[2])));
            }
            admin.createTopics(topics).all().get(60, TimeUnit.SECONDS);
            LOG.info("Created {} topics", topics.size());
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create topics", e);
        }
    }

    /**
     * Schemas are PRE-REGISTERED (v1 then v2 for accounts, v1 for orders), like scripts/register-schemas.sh does
     * in the local stack, so the subjects exist with BACKWARD compatibility before any service writes.
     *
     * <p>The services run with SCHEMA_AUTO_REGISTER=false, as in production: they never register schemas, they
     * write with the latest registered version ({@code use.latest.version}, see SchemaRegistryConfig). This also
     * proves the registered .avsc files and the generated classes stay compatible.
     */
    private void registerSchemas() {
        String sr = schemaRegistryUrl();
        registerDir(sr, "integration.account.v1-value", REPO.resolve("schemas/account"));
        registerDir(sr, "integration.order.v1-value", REPO.resolve("schemas/order"));
    }

    private static void registerDir(String sr, String subject, Path dir) {
        try {
            send(HttpRequest.newBuilder(URI.create(sr + "/config/" + subject))
                    .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                    .PUT(HttpRequest.BodyPublishers.ofString("{\"compatibility\":\"BACKWARD\"}")).build(), 200);
            List<Path> files;
            try (var s = Files.list(dir)) {
                files = s.filter(p -> p.toString().endsWith(".avsc")).sorted().toList(); // v1 before v2
            }
            for (Path f : files) {
                ObjectNode body = JSON.createObjectNode().put("schema", Files.readString(f));
                send(HttpRequest.newBuilder(URI.create(sr + "/subjects/" + subject + "/versions"))
                        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                        .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build(), 200);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void startWireMock() {
        // Same mappings as docker-compose (503 once for ...FAIL1AAA, 400 for ...REJECTAAA, 200 otherwise).
        wireMock = new WireMockServer(WireMockConfiguration.options()
                .dynamicPort()
                .usingFilesUnderDirectory(REPO.resolve("infra/wiremock").toString()));
        wireMock.start();
    }

    private void startServices() {
        Map<String, String> env = Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", bootstrapServers(),
                "KAFKA_SECURITY", "plaintext",
                "SCHEMA_REGISTRY_URL", schemaRegistryUrl(),
                "SCHEMA_AUTO_REGISTER", "false", // production setting: schemas come only from schemas/
                "DOWNSTREAM_URL", "http://localhost:" + wireMock.port() + "/downstream",
                // Short delays; the topics still are retry.1m / retry.10m.
                "RETRY_DELAY_1M", "PT2S",
                "RETRY_DELAY_10M", "PT4S");

        streams = StreamProcessorApp.start(env);
        delivery = DeliveryServiceApp.start(env);
        Awaitility.await("stream-processor RUNNING").until(() -> streams.state() == KafkaStreams.State.RUNNING);

        Properties connection = new Properties();
        connection.put("bootstrap.servers", bootstrapServers());
        ingressProducer = new KafkaProducer<>(ProducerFactory.producerProperties(connection));
        ingress = new EventIngestService(ingressProducer, IngressConfig.from(Map.of()));
    }

    /** Registers orders-source.local.json, pointing it at the Postgres network alias. */
    private void registerConnector() {
        try {
            JsonNode def = JSON.readTree(Files.readString(REPO.resolve("infra/connect/orders-source.local.json")));
            ObjectNode config = (ObjectNode) def.get("config");
            config.put("database.hostname", "postgres"); // network alias, container-to-container
            config.put("database.port", "5432");
            String base = "http://" + connect.getHost() + ":" + connect.getMappedPort(8083);
            String name = def.get("name").asText();
            send(HttpRequest.newBuilder(URI.create(base + "/connectors/" + name + "/config"))
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString(config.toString())).build(), 200, 201);

            Awaitility.await("connector and task RUNNING").until(() -> {
                try {
                    JsonNode st = JSON.readTree(get(base + "/connectors/" + name + "/status"));
                    return "RUNNING".equals(st.path("connector").path("state").asText())
                            && st.path("tasks").size() > 0
                            && st.path("tasks").findValuesAsText("state").stream().allMatch("RUNNING"::equals);
                } catch (IOException e) {
                    return false; // not ready yet
                }
            });
            // The slot is active once the snapshot is done and streaming has started: from here on, an INSERT is
            // reported as op=c (not as a snapshot read).
            Awaitility.await("Debezium streaming (replication slot active)").until(() -> {
                try (Connection c = jdbc(); Statement s = c.createStatement();
                        ResultSet rs = s.executeQuery(
                                "select active from pg_replication_slots where slot_name = 'dbz_orders_slot'")) {
                    return rs.next() && rs.getBoolean(1);
                }
            });
        } catch (IOException e) {
            throw new IllegalStateException("Cannot register connector", e);
        }
    }

    // ---- shutdown --------------------------------------------------------------------------------------------

    private void stop() {
        quietly(() -> streams.close(Duration.ofSeconds(10)));
        quietly(() -> delivery.close());
        quietly(() -> ingressProducer.close(Duration.ofSeconds(5)));
        quietly(() -> wireMock.stop());
        for (GenericContainer<?> c : new GenericContainer<?>[] {connect, schemaRegistry, postgres, kafka}) {
            if (c != null) {
                quietly(c::stop);
            }
        }
        quietly(network::close);
    }

    private static void quietly(Runnable r) {
        try {
            r.run();
        } catch (RuntimeException e) {
            LOG.warn("Ignoring error during shutdown: {}", e.toString());
        }
    }

    // ---- small HTTP helpers ----------------------------------------------------------------------------------

    private static String get(String url) throws IOException {
        return send(HttpRequest.newBuilder(URI.create(url)).GET().build(), 200);
    }

    private static String send(HttpRequest request, int... okStatuses) throws IOException {
        try {
            HttpResponse<String> r = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
            for (int ok : okStatuses) {
                if (r.statusCode() == ok) {
                    return r.body();
                }
            }
            throw new IOException(request.method() + " " + request.uri() + " -> " + r.statusCode() + " " + r.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private static long secondsSince(long nanoStart) {
        return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - nanoStart);
    }
}
