package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/arnavprasad/idem-ledger/internal/config"
	"github.com/arnavprasad/idem-ledger/internal/ledger"
	"github.com/arnavprasad/idem-ledger/internal/store"
)

func main() {
	cfg := config.Load()

	pool, err := store.NewPool(context.Background(), cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("cannot connect to database: %v", err)
	}
	defer pool.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /accounts", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name     string `json:"name"`
			Currency string `json:"currency"`
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
		account, err := store.CreateAccount(r.Context(), pool, req.Name, req.Currency)
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
		var req struct {
			FromAccount int64  `json:"from_account"`
			ToAccount   int64  `json:"to_account"`
			Amount      int64  `json:"amount"`
			Currency    string `json:"currency"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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
		t, err := ledger.Execute(r.Context(), pool, ledger.TransferRequest{
			FromAccount: req.FromAccount,
			ToAccount:   req.ToAccount,
			Amount:      req.Amount,
			Currency:    strings.ToUpper(req.Currency),
		})
		switch {
		case errors.Is(err, ledger.ErrInsufficientFunds):
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "insufficient funds"})
		case errors.Is(err, ledger.ErrAccountNotFound):
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "account not found"})
		case err != nil:
			log.Printf("execute transfer: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		default:
			writeJSON(w, http.StatusCreated, t)
		}
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
