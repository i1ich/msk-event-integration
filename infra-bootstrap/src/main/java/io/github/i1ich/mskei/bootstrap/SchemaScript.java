package io.github.i1ich.mskei.bootstrap;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the local-docker schema file (infra/postgres/init/01-schema.sql) so that it can be applied repeatedly.
 * The file itself is not changed; statements are made defensive here:
 * <ul>
 *   <li>CREATE TABLE gets IF NOT EXISTS;</li>
 *   <li>CREATE PUBLICATION is skipped when the publication exists (PostgreSQL has no IF NOT EXISTS for it);</li>
 *   <li>seed INSERTs run only while the table is empty (so a re-run does not duplicate rows).</li>
 * </ul>
 */
final class SchemaScript {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaScript.class);
    private static final Pattern CREATE_TABLE = Pattern.compile("^CREATE\\s+TABLE\\s+(?!IF\\s+NOT\\s+EXISTS)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern CREATE_PUBLICATION = Pattern.compile("^CREATE\\s+PUBLICATION\\s+(\\w+)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern INSERT_INTO = Pattern.compile("^INSERT\\s+INTO\\s+([\\w.]+)", Pattern.CASE_INSENSITIVE);

    private SchemaScript() {
    }

    /** Splits on ';' after dropping '--' comment lines. The schema file has no ';' inside literals. */
    static List<String> statements(String sql) {
        StringBuilder sb = new StringBuilder();
        for (String line : sql.split("\\R")) {
            if (!line.trim().startsWith("--")) {
                sb.append(line).append('\n');
            }
        }
        List<String> out = new ArrayList<>();
        for (String s : sb.toString().split(";")) {
            if (!s.isBlank()) {
                out.add(s.trim());
            }
        }
        return out;
    }

    static void apply(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            for (String stmt : statements(sql)) {
                Matcher pub = CREATE_PUBLICATION.matcher(stmt);
                Matcher ins = INSERT_INTO.matcher(stmt);
                if (pub.find()) {
                    if (exists(st, "SELECT 1 FROM pg_publication WHERE pubname = '" + pub.group(1) + "'")) {
                        LOG.info("publication {} exists, skipped", pub.group(1));
                        continue;
                    }
                } else if (ins.find()) {
                    if (exists(st, "SELECT 1 FROM " + ins.group(1) + " LIMIT 1")) {
                        LOG.info("{} already has rows, seed skipped", ins.group(1));
                        continue;
                    }
                } else {
                    stmt = CREATE_TABLE.matcher(stmt).replaceFirst("CREATE TABLE IF NOT EXISTS ");
                }
                st.execute(stmt);
            }
        }
    }

    private static boolean exists(Statement st, String query) throws SQLException {
        try (ResultSet rs = st.executeQuery(query)) {
            return rs.next();
        }
    }
}
