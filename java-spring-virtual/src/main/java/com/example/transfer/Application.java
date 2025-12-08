package com.example.transfer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.boot.web.embedded.tomcat.TomcatProtocolHandlerCustomizer;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}

@Configuration
class VirtualThreadConfig {
    @Bean(destroyMethod = "close")
    ExecutorService virtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean
    TomcatProtocolHandlerCustomizer<?> protocolHandlerVirtualThreads(ExecutorService executor) {
        return protocolHandler -> protocolHandler.setExecutor(executor);
    }
}

record TransferRequest(String fromAccountId, String toAccountId, double amount, String operationId) {}
record TransferResult(int status, Map<String, Object> body) {}
record LedgerEntry(String type, String accountId, double amount, String at) {}

@RestController
class TransferController {
    private final Store store;

    TransferController(Store store) {
        this.store = store;
    }

    @PostMapping("/transfer")
    ResponseEntity<Map<String, Object>> transfer(@RequestBody TransferRequest req) {
        TransferResult result = store.transfer(req);
        return ResponseEntity.status(result.status()).body(result.body());
    }

    @GetMapping("/debug/state")
    Map<String, Object> debugState() {
        return store.state();
    }
}

@Component
class Store {
    private final JdbcTemplate jdbc;

    Store(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
        seed();
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    TransferResult transfer(TransferRequest req) {
        if (req.fromAccountId() == null || req.toAccountId() == null) {
            return error("fromAccountId and toAccountId are required");
        }
        if (req.fromAccountId().equals(req.toAccountId())) {
            return error("fromAccountId and toAccountId must differ");
        }
        if (req.amount() <= 0) {
            return error("amount must be > 0");
        }

        if (req.operationId() != null) {
            Boolean exists = jdbc.queryForObject(
                    "SELECT EXISTS (SELECT 1 FROM processed_ops WHERE operation_id = ?)",
                    Boolean.class,
                    req.operationId());
            if (Boolean.TRUE.equals(exists)) {
                return ok("operation already processed", Map.of());
            }
        }

        Double fromBalance = jdbc.queryForObject(
                "SELECT balance FROM accounts WHERE id = ? FOR UPDATE",
                Double.class,
                req.fromAccountId());
        Double toBalance = jdbc.queryForObject(
                "SELECT balance FROM accounts WHERE id = ? FOR UPDATE",
                Double.class,
                req.toAccountId());

        if (fromBalance == null || toBalance == null) {
            return error("account not found");
        }
        if (fromBalance < req.amount()) {
            return error("insufficient funds");
        }

        double newFrom = fromBalance - req.amount();
        double newTo = toBalance + req.amount();

        jdbc.update("UPDATE accounts SET balance=? WHERE id=?", newFrom, req.fromAccountId());
        jdbc.update("UPDATE accounts SET balance=? WHERE id=?", newTo, req.toAccountId());

        String now = Instant.now().toString();
        jdbc.update("INSERT INTO ledger (type, account_id, amount, at) VALUES (?,?,?,?)",
                "DEBIT", req.fromAccountId(), req.amount(), now);
        jdbc.update("INSERT INTO ledger (type, account_id, amount, at) VALUES (?,?,?,?)",
                "CREDIT", req.toAccountId(), req.amount(), now);

        if (req.operationId() != null) {
            jdbc.update("INSERT INTO processed_ops (operation_id) VALUES (?)", req.operationId());
        }

        Map<String, Object> balances = new LinkedHashMap<>();
        balances.put(req.fromAccountId(), newFrom);
        balances.put(req.toAccountId(), newTo);

        return new TransferResult(200, Map.of(
                "status", "ok",
                "message", "transfer completed",
                "balances", balances));
    }

    Map<String, Object> state() {
        List<Map<String, Object>> accounts = jdbc.query(
                "SELECT id, balance FROM accounts ORDER BY id",
                accountRowMapper());
        List<Map<String, Object>> ledger = jdbc.query(
                "SELECT type, account_id, amount, at FROM ledger ORDER BY id DESC LIMIT 100",
                ledgerRowMapper());
        List<String> processed = jdbc.query(
                "SELECT operation_id FROM processed_ops ORDER BY created_at DESC LIMIT 100",
                (rs, rowNum) -> rs.getString("operation_id"));

        Map<String, Object> accMap = accounts.stream()
                .collect(Collectors.toMap(a -> (String) a.get("id"), a -> a));

        return Map.of(
                "accounts", accMap,
                "processedOps", Set.copyOf(processed),
                "ledger", ledger);
    }

    private void seed() {
        jdbc.update("INSERT INTO accounts (id, balance) VALUES ('A',1000.0),('B',500.0) ON CONFLICT (id) DO NOTHING");
    }

    private TransferResult error(String message) {
        return new TransferResult(400, Map.of("status", "error", "message", message));
    }

    private TransferResult ok(String message, Map<String, Object> extra) {
        Map<String, Object> out = new HashMap<>();
        out.put("status", "ok");
        out.put("message", message);
        out.putAll(extra);
        return new TransferResult(200, out);
    }

    private RowMapper<Map<String, Object>> accountRowMapper() {
        return (rs, rowNum) -> {
            Map<String, Object> m = new HashMap<>();
            m.put("id", rs.getString("id"));
            m.put("balance", rs.getDouble("balance"));
            return m;
        };
    }

    private RowMapper<Map<String, Object>> ledgerRowMapper() {
        return (rs, rowNum) -> {
            Map<String, Object> m = new HashMap<>();
            m.put("type", rs.getString("type"));
            m.put("accountId", rs.getString("account_id"));
            m.put("amount", rs.getDouble("amount"));
            m.put("at", rs.getString("at"));
            return m;
        };
    }
}
