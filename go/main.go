package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type TransferRequest struct {
	FromAccountID string  `json:"fromAccountId"`
	ToAccountID   string  `json:"toAccountId"`
	Amount        float64 `json:"amount"`
	OperationID   string  `json:"operationId"`
}

type TransferResponse struct {
	Status   string             `json:"status"`
	Message  string             `json:"message"`
	Balances map[string]float64 `json:"balances,omitempty"`
}

type LedgerEntry struct {
	Type      string  `json:"type"`
	AccountID string  `json:"accountId"`
	Amount    float64 `json:"amount"`
	At        string  `json:"at"`
}

type Store struct {
	pool *pgxpool.Pool
}

var (
	transferRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "transfer_requests_total",
			Help: "Total de requisições de transferência por resultado.",
		},
		[]string{"result"},
	)
	accountBalance = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "account_balance",
			Help: "Saldo atual por conta (demonstração).",
		},
		[]string{"account"},
	)
)

func init() {
	prometheus.MustRegister(transferRequests, accountBalance)
}

func main() {
	ctx := context.Background()
	dsn := buildDSN()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to open pool: %v", err)
	}
	store := &Store{pool: pool}
	if err := store.seed(ctx); err != nil {
		log.Fatalf("failed to seed database: %v", err)
	}

	http.HandleFunc("/transfer", store.handleTransfer)
	http.HandleFunc("/debug/state", store.handleDebug)
	http.Handle("/metrics", promhttp.Handler())

	log.Println("Go service listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func buildDSN() string {
	host := envOrDefault("DB_HOST", "postgres")
	port := envOrDefault("DB_PORT", "5432")
	user := envOrDefault("DB_USER", "fintech")
	pass := envOrDefault("DB_PASSWORD", "fintech")
	name := envOrDefault("DB_NAME", "fintech")
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, pass, host, port, name)
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func (s *Store) seed(ctx context.Context) error {
	// Keep default seed aligned with init.sql but idempotent
	_, err := s.pool.Exec(ctx, `
		INSERT INTO accounts (id, balance) VALUES
		('A', 1000.0),
		('B', 500.0)
		ON CONFLICT (id) DO NOTHING`)
	if err != nil {
		return err
	}
	// refresh gauges
	rows, err := s.pool.Query(ctx, "SELECT id, balance FROM accounts")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var bal float64
		if err := rows.Scan(&id, &bal); err != nil {
			return err
		}
		accountBalance.WithLabelValues(id).Set(bal)
	}
	return rows.Err()
}

func (s *Store) handleTransfer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	var req TransferRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.FromAccountID == "" || req.ToAccountID == "" {
		transferRequests.WithLabelValues("validation_error").Inc()
		writeJSON(w, http.StatusBadRequest, TransferResponse{Status: "error", Message: "fromAccountId and toAccountId are required"})
		return
	}
	if req.FromAccountID == req.ToAccountID {
		transferRequests.WithLabelValues("validation_error").Inc()
		writeJSON(w, http.StatusBadRequest, TransferResponse{Status: "error", Message: "fromAccountId and toAccountId must differ"})
		return
	}
	if req.Amount <= 0 {
		transferRequests.WithLabelValues("validation_error").Inc()
		writeJSON(w, http.StatusBadRequest, TransferResponse{Status: "error", Message: "amount must be > 0"})
		return
	}

	resp, status, err := s.transfer(r.Context(), req)
	if err != nil {
		log.Printf("transfer error: %v", err)
		writeJSON(w, status, TransferResponse{Status: "error", Message: err.Error()})
		return
	}
	writeJSON(w, status, resp)
}

func (s *Store) transfer(ctx context.Context, req TransferRequest) (TransferResponse, int, error) {
	if req.OperationID != "" {
		var exists bool
		if err := s.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM processed_ops WHERE operation_id=$1)", req.OperationID).Scan(&exists); err != nil {
			transferRequests.WithLabelValues("validation_error").Inc()
			return TransferResponse{}, http.StatusInternalServerError, fmt.Errorf("failed to check duplicate: %w", err)
		}
		if exists {
			transferRequests.WithLabelValues("duplicate").Inc()
			return TransferResponse{Status: "ok", Message: "operation already processed"}, http.StatusOK, nil
		}
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return TransferResponse{}, http.StatusInternalServerError, fmt.Errorf("failed to start tx: %w", err)
	}
	defer tx.Rollback(ctx) // safe to call after commit

	var fromBalance, toBalance float64
	if err := tx.QueryRow(ctx, "SELECT balance FROM accounts WHERE id=$1 FOR UPDATE", req.FromAccountID).Scan(&fromBalance); err != nil {
		transferRequests.WithLabelValues("account_not_found").Inc()
		if err == pgx.ErrNoRows {
			return TransferResponse{}, http.StatusBadRequest, fmt.Errorf("from account not found")
		}
		return TransferResponse{}, http.StatusInternalServerError, fmt.Errorf("load from account: %w", err)
	}
	if err := tx.QueryRow(ctx, "SELECT balance FROM accounts WHERE id=$1 FOR UPDATE", req.ToAccountID).Scan(&toBalance); err != nil {
		transferRequests.WithLabelValues("account_not_found").Inc()
		if err == pgx.ErrNoRows {
			return TransferResponse{}, http.StatusBadRequest, fmt.Errorf("to account not found")
		}
		return TransferResponse{}, http.StatusInternalServerError, fmt.Errorf("load to account: %w", err)
	}
	if fromBalance < req.Amount {
		transferRequests.WithLabelValues("insufficient_funds").Inc()
		return TransferResponse{}, http.StatusBadRequest, fmt.Errorf("insufficient funds")
	}

	fromBalance -= req.Amount
	toBalance += req.Amount

	if _, err := tx.Exec(ctx, "UPDATE accounts SET balance=$1 WHERE id=$2", fromBalance, req.FromAccountID); err != nil {
		return TransferResponse{}, http.StatusInternalServerError, fmt.Errorf("update from account: %w", err)
	}
	if _, err := tx.Exec(ctx, "UPDATE accounts SET balance=$1 WHERE id=$2", toBalance, req.ToAccountID); err != nil {
		return TransferResponse{}, http.StatusInternalServerError, fmt.Errorf("update to account: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := tx.Exec(ctx, "INSERT INTO ledger (type, account_id, amount, at) VALUES ($1,$2,$3,$4)", "DEBIT", req.FromAccountID, req.Amount, now); err != nil {
		return TransferResponse{}, http.StatusInternalServerError, fmt.Errorf("insert debit ledger: %w", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO ledger (type, account_id, amount, at) VALUES ($1,$2,$3,$4)", "CREDIT", req.ToAccountID, req.Amount, now); err != nil {
		return TransferResponse{}, http.StatusInternalServerError, fmt.Errorf("insert credit ledger: %w", err)
	}

	if req.OperationID != "" {
		if _, err := tx.Exec(ctx, "INSERT INTO processed_ops (operation_id) VALUES ($1)", req.OperationID); err != nil {
			return TransferResponse{}, http.StatusInternalServerError, fmt.Errorf("insert processed op: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return TransferResponse{}, http.StatusInternalServerError, fmt.Errorf("commit tx: %w", err)
	}

	accountBalance.WithLabelValues(req.FromAccountID).Set(fromBalance)
	accountBalance.WithLabelValues(req.ToAccountID).Set(toBalance)
	transferRequests.WithLabelValues("success").Inc()

	return TransferResponse{
		Status:  "ok",
		Message: "transfer completed",
		Balances: map[string]float64{
			req.FromAccountID: fromBalance,
			req.ToAccountID:   toBalance,
		},
	}, http.StatusOK, nil
}

func (s *Store) handleDebug(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	accounts := make(map[string]float64)
	rows, err := s.pool.Query(ctx, "SELECT id, balance FROM accounts ORDER BY id")
	if err != nil {
		http.Error(w, "failed to load accounts", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var bal float64
		if err := rows.Scan(&id, &bal); err != nil {
			http.Error(w, "failed to parse accounts", http.StatusInternalServerError)
			return
		}
		accounts[id] = bal
	}
	ledger := make([]LedgerEntry, 0)
	lrows, err := s.pool.Query(ctx, "SELECT type, account_id, amount, at FROM ledger ORDER BY id DESC LIMIT 100")
	if err == nil {
		defer lrows.Close()
		for lrows.Next() {
			var e LedgerEntry
			if err := lrows.Scan(&e.Type, &e.AccountID, &e.Amount, &e.At); err != nil {
				http.Error(w, "failed to parse ledger", http.StatusInternalServerError)
				return
			}
			ledger = append(ledger, e)
		}
	}

	processed := make([]string, 0)
	prows, err := s.pool.Query(ctx, "SELECT operation_id FROM processed_ops ORDER BY created_at DESC LIMIT 100")
	if err == nil {
		defer prows.Close()
		for prows.Next() {
			var id string
			if err := prows.Scan(&id); err != nil {
				http.Error(w, "failed to parse processed ops", http.StatusInternalServerError)
				return
			}
			processed = append(processed, id)
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"accounts":     accounts,
		"ledger":       ledger,
		"processedOps": processed,
	})
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
