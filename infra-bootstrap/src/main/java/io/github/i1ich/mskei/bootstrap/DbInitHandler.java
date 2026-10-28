package io.github.i1ich.mskei.bootstrap;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

/**
 * Custom resource: applies the orders schema (bundled infra/postgres/init/01-schema.sql) to the RDS database.
 * Also answers a direct invocation {@code {"action":"smoke-order"}} used by scripts/smoke-test.sh: it inserts,
 * updates and deletes one order, which produces three CDC events.
 *
 * <p>Env: DB_HOST, DB_NAME (default erp), DB_SECRET_ARN (JSON {"username","password"}).
 */
public class DbInitHandler implements RequestHandler<Map<String, Object>, Object> {
    private static final Logger LOG = LoggerFactory.getLogger(DbInitHandler.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    static final String PHYSICAL_ID = "db-schema";
    static final String SMOKE_ACCOUNT_ID = "001xx000004ABcdAAA";

    @FunctionalInterface
    interface ConnectionFactory {
        Connection open() throws SQLException;
    }

    @FunctionalInterface
    interface SecretReader {
        String read(String secretArn);
    }

    private final ConnectionFactory connections;
    private final String schemaSql;
    private final CustomResource.Responder responder;

    /** Lambda entry point. */
    public DbInitHandler() {
        this(defaultConnections(System.getenv(), DbInitHandler::readSecret), loadBundledSchema(),
                new CustomResource.HttpResponder());
    }

    DbInitHandler(ConnectionFactory connections, String schemaSql, CustomResource.Responder responder) {
        this.connections = connections;
        this.schemaSql = schemaSql;
        this.responder = responder;
    }

    @Override
    public Object handleRequest(Map<String, Object> event, Context context) {
        if (!event.containsKey("RequestType") && "smoke-order".equals(event.get("action"))) {
            return smokeOrder();
        }
        CustomResource.handle(event, context, responder, this::run);
        return null;
    }

    private CustomResource.Result run(CustomResource.Request req) throws Exception {
        if (req.isDelete()) {
            // Nothing to do: the database is deleted by the same stack, taking the schema with it.
            LOG.info("Delete: leaving the schema alone");
            return CustomResource.Result.of(PHYSICAL_ID);
        }
        // Create and Update: SchemaScript is idempotent, so a repeat run changes nothing.
        try (Connection conn = connections.open()) {
            SchemaScript.apply(conn, schemaSql);
        }
        return CustomResource.Result.of(PHYSICAL_ID);
    }

    /** Insert, update, delete one order (autocommit: three transactions, three change events). */
    Map<String, Object> smokeOrder() {
        try (Connection conn = connections.open()) {
            long id;
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO public.orders (account_id, status, amount, currency) "
                            + "VALUES (?, 'NEW', 10.00, 'EUR') RETURNING id")) {
                ps.setString(1, SMOKE_ACCOUNT_ID);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    id = rs.getLong(1);
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE public.orders SET status = 'PAID', updated_at = now() WHERE id = ?")) {
                ps.setLong(1, id);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM public.orders WHERE id = ?")) {
                ps.setLong(1, id);
                ps.executeUpdate();
            }
            LOG.info("smoke order {} inserted, updated, deleted", id);
            return Map.of("orderId", id);
        } catch (SQLException e) {
            throw new IllegalStateException("smoke-order failed", e);
        }
    }

    static ConnectionFactory defaultConnections(Map<String, String> env, SecretReader secrets) {
        return () -> {
            String host = require(env, "DB_HOST");
            String name = env.getOrDefault("DB_NAME", "erp");
            JsonNode secret;
            try {
                secret = JSON.readTree(secrets.read(require(env, "DB_SECRET_ARN")));
            } catch (IOException e) {
                throw new SQLException("DB secret is not valid JSON", e);
            }
            Properties props = new Properties();
            props.setProperty("user", secret.path("username").asText());
            props.setProperty("password", secret.path("password").asText());
            // RDS PostgreSQL 15+ forces TLS; "require" encrypts without needing the RDS CA bundle in the jar.
            props.setProperty("sslmode", "require");
            props.setProperty("connectTimeout", "10");
            return DriverManager.getConnection("jdbc:postgresql://" + host + ":5432/" + name, props);
        };
    }

    private static String readSecret(String arn) {
        try (SecretsManagerClient sm = SecretsManagerClient.builder()
                .httpClient(UrlConnectionHttpClient.create()).build()) {
            return sm.getSecretValue(r -> r.secretId(arn)).secretString();
        }
    }

    private static String require(Map<String, String> env, String key) {
        String v = env.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException(key + " is not set");
        }
        return v;
    }

    static String loadBundledSchema() {
        try (InputStream in = DbInitHandler.class.getResourceAsStream("/01-schema.sql")) {
            if (in == null) {
                throw new IllegalStateException("01-schema.sql is missing from the jar");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("cannot read 01-schema.sql", e);
        }
    }
}
