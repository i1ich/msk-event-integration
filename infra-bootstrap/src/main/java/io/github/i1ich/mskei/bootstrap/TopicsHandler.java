package io.github.i1ich.mskei.bootstrap;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import io.github.i1ich.mskei.common.KafkaClientConfig;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom resource: creates the Kafka topics listed in {@code topics.txt} (bundled from scripts/topics.txt).
 * CloudFormation has no native MSK topic resource.
 *
 * <p>Properties: {@code Partitions} (optional override), {@code ReplicationFactor} (optional; -1 or absent = broker
 * default, which MSK Serverless requires).
 */
public class TopicsHandler implements RequestHandler<Map<String, Object>, Void> {
    private static final Logger LOG = LoggerFactory.getLogger(TopicsHandler.class);
    static final String PHYSICAL_ID = "topics";

    /** One non-comment line of topics.txt: {@code name partitions retention_ms}. */
    record TopicSpec(String name, int partitions, long retentionMs) {
    }

    private final Supplier<Admin> adminFactory;
    private final List<TopicSpec> specs;
    private final CustomResource.Responder responder;

    /** Lambda entry point. */
    public TopicsHandler() {
        this(() -> Admin.create(KafkaClientConfig.from(System.getenv())), loadBundledSpecs(),
                new CustomResource.HttpResponder());
    }

    TopicsHandler(Supplier<Admin> adminFactory, List<TopicSpec> specs, CustomResource.Responder responder) {
        this.adminFactory = adminFactory;
        this.specs = specs;
        this.responder = responder;
    }

    @Override
    public Void handleRequest(Map<String, Object> event, Context context) {
        CustomResource.handle(event, context, responder, this::run);
        return null;
    }

    private CustomResource.Result run(CustomResource.Request req) throws Exception {
        if (req.isDelete()) {
            // Nothing to do: the topics disappear together with the MSK cluster (the topics stack is deleted
            // before the msk stack). Deleting them one by one would only slow the teardown down and could fail
            // if the cluster is already unreachable.
            LOG.info("Delete: leaving topics alone");
            return CustomResource.Result.of(PHYSICAL_ID);
        }
        // Create and Update are the same: create what is missing, never touch what exists.
        Integer partitionsOverride = intProperty(req.properties(), "Partitions");
        Integer replication = intProperty(req.properties(), "ReplicationFactor");
        Optional<Short> replicationFactor = replication == null || replication < 1
                ? Optional.empty() : Optional.of(replication.shortValue());

        try (Admin admin = adminFactory.get()) {
            int created = 0;
            for (TopicSpec spec : specs) {
                int partitions = partitionsOverride != null ? partitionsOverride : spec.partitions();
                NewTopic topic = new NewTopic(spec.name(), Optional.of(partitions), replicationFactor)
                        .configs(Map.of("retention.ms", Long.toString(spec.retentionMs())));
                if (createIfMissing(admin, topic)) {
                    created++;
                }
            }
            LOG.info("topics ready: {} created, {} already existed", created, specs.size() - created);
            return new CustomResource.Result(PHYSICAL_ID, Map.of("Created", Integer.toString(created)));
        }
    }

    private static boolean createIfMissing(Admin admin, NewTopic topic) throws InterruptedException, ExecutionException {
        try {
            admin.createTopics(List.of(topic)).values().get(topic.name()).get();
            LOG.info("created topic {}", topic.name());
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                LOG.info("topic {} already exists", topic.name());
                return false;
            }
            throw e;
        }
    }

    private static Integer intProperty(Map<String, Object> props, String key) {
        Object v = props.get(key);
        return v == null || v.toString().isBlank() ? null : Integer.valueOf(v.toString().trim());
    }

    static List<TopicSpec> loadBundledSpecs() {
        try (InputStream in = TopicsHandler.class.getResourceAsStream("/topics.txt")) {
            if (in == null) {
                throw new IllegalStateException("topics.txt is missing from the jar");
            }
            return parse(new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)).lines().toList());
        } catch (IOException e) {
            throw new IllegalStateException("cannot read topics.txt", e);
        }
    }

    static List<TopicSpec> parse(List<String> lines) {
        List<TopicSpec> out = new ArrayList<>();
        for (String raw : lines) {
            String line = raw.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            String[] f = line.split("\\s+");
            if (f.length != 3) {
                throw new IllegalArgumentException("bad topics.txt line (want: name partitions retention_ms): " + raw);
            }
            out.add(new TopicSpec(f[0], Integer.parseInt(f[1]), Long.parseLong(f[2])));
        }
        return out;
    }
}
