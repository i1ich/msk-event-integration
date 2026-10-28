package io.github.i1ich.mskei.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.lambda.runtime.Context;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DbInitHandlerTest {
    private final List<String> responses = new ArrayList<>();
    private final CustomResource.Responder responder = (u, b) -> responses.add(b);

    private static Context context() {
        Context c = mock(Context.class);
        when(c.getRemainingTimeInMillis()).thenReturn(60_000);
        return c;
    }

    private static Map<String, Object> event(String type) {
        return Map.of("RequestType", type, "ResponseURL", "http://x", "StackId", "s", "RequestId", "r",
                "LogicalResourceId", "Db");
    }

    /** Statement whose "exists" queries answer with the given flag; records executed statements. */
    private static Statement statement(boolean rowsExist, List<String> executed) throws SQLException {
        Statement st = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(rowsExist);
        when(st.executeQuery(anyString())).thenReturn(rs);
        when(st.execute(anyString())).thenAnswer(inv -> {
            executed.add(inv.getArgument(0));
            return false;
        });
        return st;
    }

    @Test
    void firstRunAppliesEverything() throws Exception {
        List<String> executed = new ArrayList<>();
        Connection conn = mock(Connection.class);
        Statement st = statement(false, executed);
        when(conn.createStatement()).thenReturn(st);

        new DbInitHandler(() -> conn, DbInitHandler.loadBundledSchema(), responder)
                .handleRequest(event("Create"), context());

        assertThat(executed).anyMatch(s -> s.startsWith("CREATE TABLE IF NOT EXISTS public.orders"));
        assertThat(executed).anyMatch(s -> s.startsWith("CREATE PUBLICATION dbz_orders_pub"));
        assertThat(executed).anyMatch(s -> s.startsWith("INSERT INTO public.orders"));
        assertThat(executed).anyMatch(s -> s.startsWith("ALTER TABLE public.orders REPLICA IDENTITY FULL"));
        assertThat(responses.get(0)).contains("SUCCESS");
        verify(conn).close();
    }

    @Test
    void rerunSkipsPublicationAndSeed() throws Exception {
        List<String> executed = new ArrayList<>();
        Connection conn = mock(Connection.class);
        Statement st = statement(true, executed);
        when(conn.createStatement()).thenReturn(st);

        new DbInitHandler(() -> conn, DbInitHandler.loadBundledSchema(), responder)
                .handleRequest(event("Update"), context());

        assertThat(executed).noneMatch(s -> s.startsWith("CREATE PUBLICATION") || s.startsWith("INSERT"));
        assertThat(executed).anyMatch(s -> s.startsWith("CREATE TABLE IF NOT EXISTS"));
        assertThat(responses.get(0)).contains("SUCCESS");
    }

    @Test
    void connectionFailureIsReportedAsFailed() {
        new DbInitHandler(() -> {
            throw new SQLException("no route");
        }, "SELECT 1", responder).handleRequest(event("Create"), context());
        assertThat(responses.get(0)).contains("FAILED").contains("no route");
    }

    @Test
    void deleteDoesNotTouchTheDatabase() {
        new DbInitHandler(() -> {
            throw new SQLException("must not connect");
        }, "x", responder).handleRequest(event("Delete"), context());
        assertThat(responses.get(0)).contains("SUCCESS");
    }

    @Test
    void smokeOrderInsertsUpdatesDeletes() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement insert = mock(PreparedStatement.class);
        PreparedStatement update = mock(PreparedStatement.class);
        PreparedStatement delete = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(rs.getLong(1)).thenReturn(42L);
        when(insert.executeQuery()).thenReturn(rs);
        when(conn.prepareStatement(org.mockito.ArgumentMatchers.startsWith("INSERT"))).thenReturn(insert);
        when(conn.prepareStatement(org.mockito.ArgumentMatchers.startsWith("UPDATE"))).thenReturn(update);
        when(conn.prepareStatement(org.mockito.ArgumentMatchers.startsWith("DELETE"))).thenReturn(delete);

        Object out = new DbInitHandler(() -> conn, "x", responder)
                .handleRequest(Map.of("action", "smoke-order"), context());

        assertThat(out).isEqualTo(Map.of("orderId", 42L));
        verify(insert).setString(1, "001xx000004ABcdAAA");
        verify(update).setLong(1, 42L);
        verify(delete).setLong(1, 42L);
        assertThat(responses).isEmpty();
    }

    @Test
    void smokeOrderWrapsSqlErrors() {
        DbInitHandler h = new DbInitHandler(() -> {
            throw new SQLException("down");
        }, "x", responder);
        assertThatThrownBy(() -> h.handleRequest(Map.of("action", "smoke-order"), context()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void defaultConnectionsValidateEnvAndSecret() {
        assertThatThrownBy(() -> DbInitHandler.defaultConnections(Map.of(), a -> "{}").open())
                .hasMessageContaining("DB_HOST");
        assertThatThrownBy(() -> DbInitHandler.defaultConnections(Map.of("DB_HOST", "h"), a -> "{}").open())
                .hasMessageContaining("DB_SECRET_ARN");
        assertThatThrownBy(() -> DbInitHandler.defaultConnections(
                Map.of("DB_HOST", "h", "DB_SECRET_ARN", "arn"), a -> "not json").open())
                .isInstanceOf(SQLException.class).hasMessageContaining("JSON");
    }

    @Test
    void statementsSplitIgnoringComments() {
        assertThat(SchemaScript.statements("-- c\nSELECT 1;\n\nSELECT 2;")).containsExactly("SELECT 1", "SELECT 2");
    }
}
