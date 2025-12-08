-- Base schema for all services
CREATE TABLE IF NOT EXISTS accounts (
    id TEXT PRIMARY KEY,
    balance NUMERIC NOT NULL
);

CREATE TABLE IF NOT EXISTS ledger (
    id BIGSERIAL PRIMARY KEY,
    type TEXT NOT NULL,
    account_id TEXT NOT NULL REFERENCES accounts(id),
    amount NUMERIC NOT NULL,
    at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS processed_ops (
    operation_id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ledger_account_at ON ledger(account_id, at DESC);

INSERT INTO accounts (id, balance) VALUES
    ('A', 1000.0),
    ('B', 500.0)
ON CONFLICT (id) DO NOTHING;
