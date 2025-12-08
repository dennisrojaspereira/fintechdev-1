
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;

public class Main {

    static class TransferOutcome {
        final int status;
        final Map<String, Object> body;

        TransferOutcome(int status, Map<String, Object> body) {
            this.status = status;
            this.body = body;
        }
    }

    static class Store {
        private final String jdbcUrl;
        private final Properties props;
        private final Map<String, Integer> transferMetrics = new HashMap<>();

        Store(String jdbcUrl, String user, String password) throws SQLException, ClassNotFoundException {
            this.jdbcUrl = jdbcUrl;
            this.props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            Class.forName("org.postgresql.Driver");
            seed();
            initMetrics();
        }

        private void initMetrics() {
            transferMetrics.put("success", 0);
            transferMetrics.put("validation_error", 0);
            transferMetrics.put("duplicate", 0);
            transferMetrics.put("account_not_found", 0);
            transferMetrics.put("insufficient_funds", 0);
        }

        private Connection conn() throws SQLException {
            return DriverManager.getConnection(jdbcUrl, props);
        }

        void seed() throws SQLException {
            try (Connection c = conn();
                 PreparedStatement ps = c.prepareStatement("INSERT INTO accounts (id, balance) VALUES ('A',1000.0),('B',500.0) ON CONFLICT (id) DO NOTHING")) {
                ps.executeUpdate();
            }
        }

        TransferOutcome transfer(Map<String, Object> json) {
            String fromAccountId = (String) json.get("fromAccountId");
            String toAccountId = (String) json.get("toAccountId");
            Number amountNum = (Number) json.get("amount");
            String operationId = (String) json.get("operationId");

            if (fromAccountId == null || toAccountId == null) {
                incMetric("validation_error");
                return new TransferOutcome(400, Map.of("status", "error", "message", "fromAccountId and toAccountId are required"));
            }
            if (fromAccountId.equals(toAccountId)) {
                incMetric("validation_error");
                return new TransferOutcome(400, Map.of("status", "error", "message", "fromAccountId and toAccountId must differ"));
            }
            if (amountNum == null || amountNum.doubleValue() <= 0) {
                incMetric("validation_error");
                return new TransferOutcome(400, Map.of("status", "error", "message", "amount must be > 0"));
            }
            double amount = amountNum.doubleValue();

            try (Connection c = conn()) {
                c.setAutoCommit(false);

                if (operationId != null) {
                    try (PreparedStatement ps = c.prepareStatement("SELECT 1 FROM processed_ops WHERE operation_id=?")) {
                        ps.setString(1, operationId);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                incMetric("duplicate");
                                c.rollback();
                                return new TransferOutcome(200, Map.of("status", "ok", "message", "operation already processed"));
                            }
                        }
                    }
                }

                Double fromBalance = balanceForUpdate(c, fromAccountId);
                Double toBalance = balanceForUpdate(c, toAccountId);
                if (fromBalance == null || toBalance == null) {
                    incMetric("account_not_found");
                    c.rollback();
                    return new TransferOutcome(400, Map.of("status", "error", "message", "account not found"));
                }
                if (fromBalance < amount) {
                    incMetric("insufficient_funds");
                    c.rollback();
                    return new TransferOutcome(400, Map.of("status", "error", "message", "insufficient funds"));
                }

                double newFrom = fromBalance - amount;
                double newTo = toBalance + amount;

                try (PreparedStatement ps = c.prepareStatement("UPDATE accounts SET balance=? WHERE id=?")) {
                    ps.setDouble(1, newFrom);
                    ps.setString(2, fromAccountId);
                    ps.executeUpdate();
                    ps.setDouble(1, newTo);
                    ps.setString(2, toAccountId);
                    ps.executeUpdate();
                }

                String now = Instant.now().toString();
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO ledger (type, account_id, amount, at) VALUES (?,?,?,?)")) {
                    ps.setString(1, "DEBIT");
                    ps.setString(2, fromAccountId);
                    ps.setDouble(3, amount);
                    ps.setString(4, now);
                    ps.executeUpdate();
                    ps.setString(1, "CREDIT");
                    ps.setString(2, toAccountId);
                    ps.setDouble(3, amount);
                    ps.setString(4, now);
                    ps.executeUpdate();
                }

                if (operationId != null) {
                    try (PreparedStatement ps = c.prepareStatement("INSERT INTO processed_ops (operation_id) VALUES (?)")) {
                        ps.setString(1, operationId);
                        ps.executeUpdate();
                    }
                }

                c.commit();
                incMetric("success");

                Map<String, Object> balances = new HashMap<>();
                balances.put(fromAccountId, newFrom);
                balances.put(toAccountId, newTo);
                return new TransferOutcome(200, Map.of(
                        "status", "ok",
                        "message", "transfer completed",
                        "balances", balances
                ));
            } catch (SQLException e) {
                incMetric("validation_error");
                return new TransferOutcome(500, Map.of("status", "error", "message", "database error"));
            }
        }

        private Double balanceForUpdate(Connection c, String id) throws SQLException {
            try (PreparedStatement ps = c.prepareStatement("SELECT balance FROM accounts WHERE id=? FOR UPDATE")) {
                ps.setString(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getDouble("balance");
                    }
                }
            }
            return null;
        }

        Map<String, Object> state() throws SQLException {
            Map<String, Object> accs = new HashMap<>();
            List<Map<String, Object>> ledger = new ArrayList<>();
            Set<String> processed = new LinkedHashSet<>();

            try (Connection c = conn()) {
                try (PreparedStatement ps = c.prepareStatement("SELECT id, balance FROM accounts ORDER BY id"); ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String id = rs.getString("id");
                        accs.put(id, Map.of("id", id, "balance", rs.getDouble("balance")));
                    }
                }

                try (PreparedStatement ps = c.prepareStatement("SELECT type, account_id, amount, at FROM ledger ORDER BY id DESC LIMIT 100"); ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ledger.add(Map.of(
                                "type", rs.getString("type"),
                                "accountId", rs.getString("account_id"),
                                "amount", rs.getDouble("amount"),
                                "at", rs.getString("at")));
                    }
                }

                try (PreparedStatement ps = c.prepareStatement("SELECT operation_id FROM processed_ops ORDER BY created_at DESC LIMIT 100"); ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        processed.add(rs.getString("operation_id"));
                    }
                }
            }
            Map<String, Object> out = new HashMap<>();
            out.put("accounts", accs);
            out.put("ledger", ledger);
            out.put("processedOps", processed);
            return out;
        }

        String metrics() throws SQLException {
            StringBuilder sb = new StringBuilder();
            sb.append("# TYPE transfer_requests_total counter\n");
            for (var e : transferMetrics.entrySet()) {
                sb.append("transfer_requests_total{result=\"")
                        .append(e.getKey())
                        .append("\"} ")
                        .append(e.getValue())
                        .append("\n");
            }
            sb.append("\n# TYPE account_balance gauge\n");
            try (Connection c = conn(); PreparedStatement ps = c.prepareStatement("SELECT id, balance FROM accounts"); ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    sb.append("account_balance{account=\"")
                            .append(rs.getString("id"))
                            .append("\"} ")
                            .append(rs.getDouble("balance"))
                            .append("\n");
                }
            }
            return sb.toString();
        }

        private void incMetric(String key) {
            transferMetrics.merge(key, 1, Integer::sum);
        }
    }

    // Extremely minimal JSON helper (not robust, but fine for controlled payload)
    static class Json {
        @SuppressWarnings("unchecked")
        static Map<String, Object> parse(String body) {
            body = body.trim();
            if (!body.startsWith("{") || !body.endsWith("}")) {
                return new HashMap<>();
            }
            Map<String, Object> map = new HashMap<>();
            String inner = body.substring(1, body.length() - 1).trim();
            if (inner.isEmpty()) return map;
            String[] parts = inner.split(",");
            for (String part : parts) {
                String[] kv = part.split(":", 2);
                if (kv.length != 2) continue;
                String key = kv[0].trim().replaceAll("^\"|\"$", "");
                String value = kv[1].trim();
                if (value.startsWith("\"") && value.endsWith("\"")) {
                    map.put(key, value.substring(1, value.length() - 1));
                } else {
                    try {
                        map.put(key, Double.parseDouble(value));
                    } catch (NumberFormatException e) {
                        map.put(key, value);
                    }
                }
            }
            return map;
        }

        static String stringify(Map<String, Object> map) {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            boolean first = true;
            for (var e : map.entrySet()) {
                if (!first) sb.append(",");
                first = false;
                sb.append("\"").append(e.getKey()).append("\":").append(toJsonValue(e.getValue()));
            }
            sb.append("}");
            return sb.toString();
        }

        @SuppressWarnings("unchecked")
        private static String toJsonValue(Object v) {
            if (v == null) return "null";
            if (v instanceof String) {
                return "\"" + ((String) v).replace("\"", "\\\"") + "\"";
            }
            if (v instanceof Number || v instanceof Boolean) {
                return v.toString();
            }
            if (v instanceof Map) {
                return stringify((Map<String, Object>) v);
            }
            if (v instanceof Collection) {
                StringBuilder sb = new StringBuilder();
                sb.append("[");
                boolean first = true;
                for (Object o : (Collection<?>) v) {
                    if (!first) sb.append(",");
                    first = false;
                    sb.append(toJsonValue(o));
                }
                sb.append("]");
                return sb.toString();
            }
            return "\"" + v.toString() + "\"";
        }
    }

    public static void main(String[] args) throws Exception {
        String host = envOr("DB_HOST", "postgres");
        String port = envOr("DB_PORT", "5432");
        String user = envOr("DB_USER", "fintech");
        String pass = envOr("DB_PASSWORD", "fintech");
        String name = envOr("DB_NAME", "fintech");
        String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s", host, port, name);

        Store store = new Store(jdbcUrl, user, pass);

        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        server.createContext("/transfer", exchange -> {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            String body = readBody(exchange);
            TransferOutcome outcome = store.transfer(Json.parse(body));
            String resp = Json.stringify(outcome.body);
            writeJson(exchange, outcome.status, resp);
        });

        server.createContext("/debug/state", exchange -> {
            try {
                String resp = Json.stringify(store.state());
                writeJson(exchange, 200, resp);
            } catch (SQLException e) {
                writeJson(exchange, 500, Json.stringify(Map.of("error", "database error")));
            }
        });

        server.createContext("/metrics", exchange -> {
            try {
                String body = store.metrics();
                Headers h = exchange.getResponseHeaders();
                h.set("Content-Type", "text/plain; version=0.0.4");
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            } catch (SQLException e) {
                writeJson(exchange, 500, Json.stringify(Map.of("error", "database error")));
            }
        });

        server.setExecutor(Executors.newFixedThreadPool(8));
        System.out.println("Java service listening on :8080");
        server.start();
    }

    private static String envOr(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? fallback : v;
    }

    private static String readBody(HttpExchange exchange) throws IOException {
        InputStream is = exchange.getRequestBody();
        byte[] buf = is.readAllBytes();
        return new String(buf, StandardCharsets.UTF_8);
    }

    private static void writeJson(HttpExchange exchange, int status, String body) throws IOException {
        Headers h = exchange.getResponseHeaders();
        h.set("Content-Type", "application/json");
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
