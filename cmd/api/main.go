package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/arnavprasad/idem-ledger/internal/config"
	"github.com/arnavprasad/idem-ledger/internal/idempotency"
	"github.com/arnavprasad/idem-ledger/internal/ledger"
	"github.com/arnavprasad/idem-ledger/internal/outbox"
	"github.com/arnavprasad/idem-ledger/internal/store"
)

func main() {
	cfg := config.Load()

	pool, err := store.NewPool(context.Background(), cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("cannot connect to database: %v", err)
	}
	defer pool.Close()

	// Start the outbox poller in the background. It delivers events to merchant
	// webhooks at-least-once; the poller goroutine lives for the server's lifetime.
	pollerCtx, cancelPoller := context.WithCancel(context.Background())
	defer cancelPoller()
	go outbox.New(pool).Run(pollerCtx)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /accounts", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name       string `json:"name"`
			Currency   string `json:"currency"`
			WebhookURL string `json:"webhook_url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
			return
		}
		req.Name = strings.TrimSpace(req.Name)
		req.Currency = strings.ToUpper(strings.TrimSpace(req.Currency))
		if req.Name == "" {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "name is required"})
			return
		}
		if len(req.Currency) != 3 {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "currency must be a 3-letter ISO 4217 code"})
			return
		}
		var webhookURL *string
		if req.WebhookURL != "" {
			webhookURL = &req.WebhookURL
		}
		account, err := store.CreateAccount(r.Context(), pool, req.Name, req.Currency, webhookURL)
		if err != nil {
			log.Printf("create account: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
			return
		}
		writeJSON(w, http.StatusCreated, account)
	})

	mux.HandleFunc("GET /accounts/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid account id"})
			return
		}
		account, err := store.GetAccount(r.Context(), pool, id)
		if errors.Is(err, store.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "account not found"})
			return
		}
		if err != nil {
			log.Printf("get account: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
			return
		}
		writeJSON(w, http.StatusOK, account)
	})

	mux.HandleFunc("GET /accounts/{id}/history", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid account id"})
			return
		}
		afterID := int64(0)
		if s := r.URL.Query().Get("after"); s != "" {
			afterID, err = strconv.ParseInt(s, 10, 64)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid after cursor"})
				return
			}
		}
		postings, err := store.GetPostings(r.Context(), pool, id, afterID, 50)
		if err != nil {
			log.Printf("get postings: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
			return
		}
		if postings == nil {
			postings = []store.Posting{}
		}
		writeJSON(w, http.StatusOK, map[string]any{"postings": postings})
	})

	mux.HandleFunc("POST /transfers", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read request body"})
			return
		}

		var req struct {
			FromAccount int64  `json:"from_account"`
			ToAccount   int64  `json:"to_account"`
			Amount      int64  `json:"amount"`
			Currency    string `json:"currency"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
			return
		}
		if req.Amount <= 0 {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "amount must be positive"})
			return
		}
		if req.FromAccount == req.ToAccount {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "from_account and to_account must differ"})
			return
		}
		if len(req.Currency) != 3 {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "currency must be a 3-letter ISO 4217 code"})
			return
		}

		idemKey := r.Header.Get("Idempotency-Key")
		lreq := ledger.TransferRequest{
			FromAccount: req.FromAccount,
			ToAccount:   req.ToAccount,
			Amount:      req.Amount,
			Currency:    strings.ToUpper(req.Currency),
		}
		if idemKey != "" {
			lreq.IdempotencyKey = idemKey
			lreq.RequestHash = idempotency.HashRequest(body)
		}

		t, stored, err := ledger.Execute(r.Context(), pool, lreq)
		switch {
		case errors.Is(err, idempotency.ErrDuplicateRequest):
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "idempotency key already used with a different request"})
		case errors.Is(err, idempotency.ErrInProgress):
			writeJSON(w, http.StatusConflict, map[string]string{"error": "request already in progress"})
		case errors.Is(err, ledger.ErrInsufficientFunds):
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "insufficient funds"})
		case errors.Is(err, ledger.ErrAccountNotFound):
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "account not found"})
		case err != nil:
			log.Printf("execute transfer: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		case stored != nil:
			// Replay: return exactly the response we stored the first time.
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Idempotent-Replayed", "true")
			w.WriteHeader(stored.Code)
			w.Write(stored.Body)
			w.Write([]byte("\n"))
		default:
			writeJSON(w, http.StatusCreated, t)
		}
	})

	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		m, err := store.GetOutboxMetrics(r.Context(), pool)
		if err != nil {
			log.Printf("get metrics: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"outbox": m})
	})

	log.Printf("server listening on :%s", cfg.Port)
	if err := http.ListenAndServe(":"+cfg.Port, mux); err != nil {
		log.Fatal(err)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
