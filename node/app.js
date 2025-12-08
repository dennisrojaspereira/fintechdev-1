
const express = require("express");
const client = require("prom-client");
const { Pool } = require("pg");

const app = express();
app.use(express.json());

client.collectDefaultMetrics();
const transferCounter = new client.Counter({
  name: "transfer_requests_total",
  help: "Total de requisições de transferência por resultado",
  labelNames: ["result"],
});

const pool = new Pool({
  host: process.env.DB_HOST || "postgres",
  port: Number(process.env.DB_PORT || 5432),
  user: process.env.DB_USER || "fintech",
  password: process.env.DB_PASSWORD || "fintech",
  database: process.env.DB_NAME || "fintech",
  max: 10,
});

async function seed() {
  await pool.query(
    "INSERT INTO accounts (id, balance) VALUES ($1,$2),($3,$4) ON CONFLICT (id) DO NOTHING",
    ["A", 1000, "B", 500]
  );
}

async function loadAccounts() {
  const { rows } = await pool.query("SELECT id, balance FROM accounts ORDER BY id");
  return rows.reduce((acc, row) => {
    acc[row.id] = { id: row.id, balance: Number(row.balance) };
    return acc;
  }, {});
}

async function loadLedger(limit = 100) {
  const { rows } = await pool.query(
    "SELECT type, account_id, amount, at FROM ledger ORDER BY id DESC LIMIT $1",
    [limit]
  );
  return rows.map((r) => ({
    type: r.type,
    accountId: r.account_id,
    amount: Number(r.amount),
    at: r.at,
  }));
}

app.post("/transfer", async (req, res) => {
  const { fromAccountId, toAccountId, amount, operationId } = req.body || {};

  if (!fromAccountId || !toAccountId) {
    transferCounter.inc({ result: "validation_error" });
    return res.status(400).json({ status: "error", message: "fromAccountId and toAccountId are required" });
  }
  if (fromAccountId === toAccountId) {
    transferCounter.inc({ result: "validation_error" });
    return res.status(400).json({ status: "error", message: "fromAccountId and toAccountId must differ" });
  }
  if (typeof amount !== "number" || isNaN(amount) || amount <= 0) {
    transferCounter.inc({ result: "validation_error" });
    return res.status(400).json({ status: "error", message: "amount must be > 0" });
  }

  const clientPg = await pool.connect();
  try {
    await clientPg.query("BEGIN");

    if (operationId) {
      const dup = await clientPg.query(
        "SELECT 1 FROM processed_ops WHERE operation_id=$1",
        [operationId]
      );
      if (dup.rowCount > 0) {
        transferCounter.inc({ result: "duplicate" });
        await clientPg.query("ROLLBACK");
        return res.json({ status: "ok", message: "operation already processed" });
      }
    }

    const fromRow = await clientPg.query(
      "SELECT balance FROM accounts WHERE id=$1 FOR UPDATE",
      [fromAccountId]
    );
    const toRow = await clientPg.query(
      "SELECT balance FROM accounts WHERE id=$1 FOR UPDATE",
      [toAccountId]
    );

    if (fromRow.rowCount === 0 || toRow.rowCount === 0) {
      transferCounter.inc({ result: "account_not_found" });
      await clientPg.query("ROLLBACK");
      return res.status(400).json({ status: "error", message: "account not found" });
    }

    const fromBalance = Number(fromRow.rows[0].balance);
    const toBalance = Number(toRow.rows[0].balance);

    if (fromBalance < amount) {
      transferCounter.inc({ result: "insufficient_funds" });
      await clientPg.query("ROLLBACK");
      return res.status(400).json({ status: "error", message: "insufficient funds" });
    }

    const newFrom = fromBalance - amount;
    const newTo = toBalance + amount;

    await clientPg.query("UPDATE accounts SET balance=$1 WHERE id=$2", [newFrom, fromAccountId]);
    await clientPg.query("UPDATE accounts SET balance=$1 WHERE id=$2", [newTo, toAccountId]);

    const now = new Date().toISOString();
    await clientPg.query(
      "INSERT INTO ledger (type, account_id, amount, at) VALUES ($1,$2,$3,$4)",
      ["DEBIT", fromAccountId, amount, now]
    );
    await clientPg.query(
      "INSERT INTO ledger (type, account_id, amount, at) VALUES ($1,$2,$3,$4)",
      ["CREDIT", toAccountId, amount, now]
    );

    if (operationId) {
      await clientPg.query(
        "INSERT INTO processed_ops (operation_id) VALUES ($1)",
        [operationId]
      );
    }

    await clientPg.query("COMMIT");
    transferCounter.inc({ result: "success" });

    return res.json({
      status: "ok",
      message: "transfer completed",
      balances: {
        [fromAccountId]: newFrom,
        [toAccountId]: newTo,
      },
    });
  } catch (err) {
    await clientPg.query("ROLLBACK");
    console.error("transfer error", err);
    return res.status(500).json({ status: "error", message: "internal error" });
  } finally {
    clientPg.release();
  }
});

app.get("/debug/state", async (_req, res) => {
  try {
    const [accounts, ledger, processedRows] = await Promise.all([
      loadAccounts(),
      loadLedger(100),
      pool.query("SELECT operation_id FROM processed_ops ORDER BY created_at DESC LIMIT 100"),
    ]);
    res.json({
      accounts,
      ledger,
      processedOps: processedRows.rows.map((r) => r.operation_id),
    });
  } catch (err) {
    console.error("debug/state error", err);
    res.status(500).json({ error: "internal error" });
  }
});

app.get("/metrics", async (_req, res) => {
  res.set("Content-Type", client.register.contentType);
  res.end(await client.register.metrics());
});

const PORT = 3000;
seed()
  .then(() => {
    app.listen(PORT, () => {
      console.log(`Node service listening on :${PORT}`);
    });
  })
  .catch((err) => {
    console.error("failed to seed", err);
    process.exit(1);
  });
