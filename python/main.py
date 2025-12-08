
import os
from datetime import datetime
from typing import Dict, List

import psycopg
from psycopg_pool import ConnectionPool
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

app = FastAPI(title="Fintech Python")

transfer_requests = Counter(
    "transfer_requests_total",
    "Total de requisições de transferência por resultado",
    labelnames=["result"],
)
account_balance = Gauge(
    "account_balance",
    "Saldo atual por conta (demonstração)",
    labelnames=["account"],
)

class TransferRequest(BaseModel):
    from_account_id: str = Field(..., alias="fromAccountId")
    to_account_id: str = Field(..., alias="toAccountId")
    amount: float
    operation_id: str | None = Field(None, alias="operationId")

class TransferResponse(BaseModel):
    status: str
    message: str
    balances: Dict[str, float] | None = None

class LedgerEntry(BaseModel):
    type: str
    accountId: str
    amount: float
    at: str

def build_dsn() -> str:
    host = os.getenv("DB_HOST", "postgres")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USER", "fintech")
    password = os.getenv("DB_PASSWORD", "fintech")
    name = os.getenv("DB_NAME", "fintech")
    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


class Store:
    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    def seed(self) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "INSERT INTO accounts (id, balance) VALUES (%s,%s),(%s,%s) ON CONFLICT (id) DO NOTHING",
                ("A", 1000.0, "B", 500.0),
            )
            for row in conn.execute("SELECT id, balance FROM accounts"):
                account_balance.labels(account=row[0]).set(float(row[1]))

    def transfer(self, req: TransferRequest) -> TransferResponse:
        if req.from_account_id == "" or req.to_account_id == "":
            transfer_requests.labels(result="validation_error").inc()
            raise ValueError("fromAccountId and toAccountId are required")
        if req.from_account_id == req.to_account_id:
            transfer_requests.labels(result="validation_error").inc()
            raise ValueError("fromAccountId and toAccountId must differ")
        if req.amount <= 0:
            transfer_requests.labels(result="validation_error").inc()
            raise ValueError("amount must be > 0")

        with self.pool.connection() as conn:
            with conn.transaction():
                if req.operation_id:
                    dup = conn.execute(
                        "SELECT 1 FROM processed_ops WHERE operation_id=%s",
                        (req.operation_id,),
                    ).fetchone()
                    if dup:
                        transfer_requests.labels(result="duplicate").inc()
                        return TransferResponse(status="ok", message="operation already processed")

                from_row = conn.execute(
                    "SELECT balance FROM accounts WHERE id=%s FOR UPDATE",
                    (req.from_account_id,),
                ).fetchone()
                to_row = conn.execute(
                    "SELECT balance FROM accounts WHERE id=%s FOR UPDATE",
                    (req.to_account_id,),
                ).fetchone()

                if not from_row or not to_row:
                    transfer_requests.labels(result="account_not_found").inc()
                    raise ValueError("account not found")

                from_balance = float(from_row[0])
                to_balance = float(to_row[0])

                if from_balance < req.amount:
                    transfer_requests.labels(result="insufficient_funds").inc()
                    raise ValueError("insufficient funds")

                new_from = from_balance - req.amount
                new_to = to_balance + req.amount

                conn.execute(
                    "UPDATE accounts SET balance=%s WHERE id=%s",
                    (new_from, req.from_account_id),
                )
                conn.execute(
                    "UPDATE accounts SET balance=%s WHERE id=%s",
                    (new_to, req.to_account_id),
                )

                now = datetime.utcnow().isoformat()
                conn.execute(
                    "INSERT INTO ledger (type, account_id, amount, at) VALUES (%s,%s,%s,%s)",
                    ("DEBIT", req.from_account_id, req.amount, now),
                )
                conn.execute(
                    "INSERT INTO ledger (type, account_id, amount, at) VALUES (%s,%s,%s,%s)",
                    ("CREDIT", req.to_account_id, req.amount, now),
                )

                if req.operation_id:
                    conn.execute(
                        "INSERT INTO processed_ops (operation_id) VALUES (%s)",
                        (req.operation_id,),
                    )

                account_balance.labels(account=req.from_account_id).set(new_from)
                account_balance.labels(account=req.to_account_id).set(new_to)
                transfer_requests.labels(result="success").inc()

                return TransferResponse(
                    status="ok",
                    message="transfer completed",
                    balances={req.from_account_id: new_from, req.to_account_id: new_to},
                )

    def state(self) -> Dict[str, object]:
        with self.pool.connection() as conn:
            accounts: Dict[str, Dict[str, float]] = {}
            for row in conn.execute("SELECT id, balance FROM accounts ORDER BY id"):
                accounts[row[0]] = {"id": row[0], "balance": float(row[1])}

            ledger: List[Dict[str, object]] = []
            for row in conn.execute(
                "SELECT type, account_id, amount, at FROM ledger ORDER BY id DESC LIMIT 100"
            ):
                ledger.append(
                    {
                        "type": row[0],
                        "accountId": row[1],
                        "amount": float(row[2]),
                        "at": row[3].isoformat() if hasattr(row[3], "isoformat") else row[3],
                    }
                )

            processed = [row[0] for row in conn.execute(
                "SELECT operation_id FROM processed_ops ORDER BY created_at DESC LIMIT 100"
            )]

            return {"accounts": accounts, "ledger": ledger, "processedOps": processed}


pool = ConnectionPool(build_dsn(), min_size=1, max_size=10, open=False)
pool.open()
store = Store(pool)
store.seed()


@app.post("/transfer", response_model=TransferResponse)
def transfer(req: TransferRequest):
    try:
        return store.transfer(req)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except psycopg.Error as e:  # guard unexpected driver errors
        raise HTTPException(status_code=500, detail="database error") from e


@app.get("/debug/state")
def debug_state():
    try:
        return store.state()
    except psycopg.Error:
        raise HTTPException(status_code=500, detail="database error")


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
