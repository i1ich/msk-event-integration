package io.github.i1ich.mskei.it.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Proves the registry enforces BACKWARD compatibility on the account subject. Self-contained. */
class SchemaRegistryCompatibilityIT {

  private static final String CT = "application/vnd.schemaregistry.v1+json";
  private static final String SUBJECT = "integration.account.v1-value";
  private static final Path SCHEMAS = Path.of("..", "schemas");
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  private static Network network;
  private static GenericContainer<?> kafka;
  private static GenericContainer<?> registry;
  private static String base;

  @BeforeAll
  static void start() {
    network = Network.newNetwork();
    // Plain single-node KRaft broker: only Schema Registry (same network) talks to it.
    // (Listeners use the "kafka" alias, not 0.0.0.0: cp-kafka 7.9 rejects 0.0.0.0 here, as in docker-compose.)
    kafka =
        new GenericContainer<>(DockerImageName.parse("confluentinc/cp-kafka:7.9.0"))
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withEnv("CLUSTER_ID", "4L6g3nShT-eMCtK--X86sw")
            .withEnv("KAFKA_NODE_ID", "1")
            .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
            .withEnv("KAFKA_LISTENERS", "PLAINTEXT://kafka:19092,CONTROLLER://kafka:9093")
            .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://kafka:19092")
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT")
            .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka:9093")
            .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1))
            .withStartupTimeout(Duration.ofMinutes(3));
    kafka.start();
    registry =
        new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.9.0"))
            .withNetwork(network)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(3));
    registry.start();
    base = "http://" + registry.getHost() + ":" + registry.getMappedPort(8081);
  }

  @AfterAll
  static void stop() {
    if (registry != null) registry.stop();
    if (kafka != null) kafka.stop();
    if (network != null) network.close();
  }

  @Test
  void backwardCompatibilityIsEnforced() throws Exception {
    assertThat(send("PUT", "/config/" + SUBJECT, "{\"compatibility\":\"BACKWARD\"}").statusCode())
        .isEqualTo(200);

    String v1 = payload("account/AccountEvent.v1.avsc");
    String v2 = payload("account/AccountEvent.v2.avsc");
    String bad = payload("test-fixtures/AccountEvent.incompatible.avsc");

    assertThat(send("POST", "/subjects/" + SUBJECT + "/versions", v1).statusCode()).isEqualTo(200);

    HttpResponse<String> v2Check =
        send("POST", "/compatibility/subjects/" + SUBJECT + "/versions/latest", v2);
    assertThat(v2Check.statusCode()).isEqualTo(200);
    assertThat(isCompatible(v2Check)).isTrue();
    assertThat(send("POST", "/subjects/" + SUBJECT + "/versions", v2).statusCode()).isEqualTo(200);

    HttpResponse<String> badCheck =
        send("POST", "/compatibility/subjects/" + SUBJECT + "/versions/latest", bad);
    assertThat(badCheck.statusCode()).isEqualTo(200);
    assertThat(isCompatible(badCheck)).isFalse();
    assertThat(send("POST", "/subjects/" + SUBJECT + "/versions", bad).statusCode()).isEqualTo(409);

    // the rejected schema must not have created a version
    assertThat(JSON.readTree(send("GET", "/subjects/" + SUBJECT + "/versions", null).body()))
        .hasSize(2);
  }

  @Test
  void orderSchemaRegisters() throws Exception {
    String subject = "integration.order.v1-value";
    HttpResponse<String> r =
        send("POST", "/subjects/" + subject + "/versions", payload("order/OrderEvent.v1.avsc"));
    assertThat(r.statusCode()).isEqualTo(200);
    assertThat(JSON.readTree(r.body()).has("id")).isTrue();
  }

  private static boolean isCompatible(HttpResponse<String> r) throws Exception {
    JsonNode n = JSON.readTree(r.body());
    return n.get("is_compatible").asBoolean();
  }

  private static String payload(String relative) throws Exception {
    String schema = Files.readString(SCHEMAS.resolve(relative));
    return JSON.writeValueAsString(JSON.createObjectNode().put("schema", schema));
  }

  private static HttpResponse<String> send(String method, String path, String body)
      throws Exception {
    HttpRequest.Builder b =
        HttpRequest.newBuilder(URI.create(base + path)).header("Content-Type", CT);
    b.method(
        method,
        body == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(body));
    return HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }
}
