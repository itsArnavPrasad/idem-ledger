package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

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
	go outbox.New(pool).Run(pollerCtx)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "db_unavailable", "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /accounts", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
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
			if err := validateWebhookURL(req.WebhookURL); err != nil {
				writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
				return
			}
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
		// Verify the account exists before returning its history.
		// Without this check, an unknown account returns 200 {"postings":[]} — hiding typos.
		if _, err := store.GetAccount(r.Context(), pool, id); errors.Is(err, store.ErrNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "account not found"})
			return
		} else if err != nil {
			log.Printf("get account for history: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
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
		// Require idempotency key — without it, a network retry after the server commits
		// would create a duplicate transfer (double spend). Stripe enforces this too.
		idemKey := r.Header.Get("Idempotency-Key")
		if idemKey == "" {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "Idempotency-Key header is required"})
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
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

		lreq := ledger.TransferRequest{
			FromAccount:    req.FromAccount,
			ToAccount:      req.ToAccount,
			Amount:         req.Amount,
			Currency:       strings.ToUpper(req.Currency),
			IdempotencyKey: idemKey,
			RequestHash:    idempotency.HashRequest(body),
		}

		var t ledger.Transfer
		var stored *idempotency.StoredResponse
		switch cfg.Strategy {
		case "select_for_update":
			t, stored, err = ledger.ExecuteWithForUpdate(r.Context(), pool, lreq)
		case "optimistic":
			// Optimistic strategy does not support idempotency keys — the multi-transaction
			// retry loop is incompatible with single-transaction idempotency claims.
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "optimistic strategy does not support idempotency keys; set STRATEGY=conditional_update or STRATEGY=select_for_update"})
			return
		default: // "conditional_update"
			t, stored, err = ledger.Execute(r.Context(), pool, lreq)
		}

		switch {
		case errors.Is(err, idempotency.ErrDuplicateRequest):
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "idempotency key already used with a different request"})
		case errors.Is(err, idempotency.ErrInProgress):
			writeJSON(w, http.StatusConflict, map[string]string{"error": "request already in progress"})
		case errors.Is(err, ledger.ErrInsufficientFunds):
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "insufficient funds"})
		case errors.Is(err, ledger.ErrAccountNotFound):
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "account not found"})
		case errors.Is(err, ledger.ErrCurrencyMismatch),
			errors.Is(err, ledger.ErrInvalidAmount),
			errors.Is(err, ledger.ErrSameAccount),
			errors.Is(err, ledger.ErrInvalidCurrency):
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
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

	srv := &http.Server{Addr: ":" + cfg.Port, Handler: mux}

	// Graceful shutdown: SIGTERM / SIGINT → drain in-flight HTTP requests (15s budget),
	// then stop the outbox poller. The poller runs on context.Background() so it can
	// outlive the HTTP server if needed; we cancel it after HTTP drains.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		log.Printf("shutdown signal received — draining HTTP (15s timeout)...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown: %v", err)
		}
		cancelPoller()
	}()

	log.Printf("server listening on :%s (strategy=%s)", cfg.Port, cfg.Strategy)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// validateWebhookURL rejects URLs that could enable SSRF attacks from the outbox poller.
// The poller POSTs to whatever URL is stored — an attacker who can create accounts could
// point webhooks at internal services (AWS metadata endpoint, internal APIs, etc.).
func validateWebhookURL(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return errors.New("webhook_url is not a valid URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("webhook_url must use http or https scheme")
	}
	hostname := u.Hostname()
	if hostname == "" {
		return errors.New("webhook_url must have a hostname")
	}

	// Resolve the hostname and reject private/loopback/link-local ranges.
	// This blocks 127.x, 10.x, 172.16-31.x, 192.168.x, 169.254.x (AWS metadata), ::1.
	addrs, err := net.LookupHost(hostname)
	if err != nil {
		return errors.New("webhook_url hostname could not be resolved")
	}
	privateRanges := []string{
		"127.0.0.0/8",
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"169.254.0.0/16",
		"::1/128",
		"fc00::/7",
	}
	var blockedNets []*net.IPNet
	for _, cidr := range privateRanges {
		_, ipNet, _ := net.ParseCIDR(cidr)
		blockedNets = append(blockedNets, ipNet)
	}
	for _, addr := range addrs {
		ip := net.ParseIP(addr)
		if ip == nil {
			continue
		}
		for _, blocked := range blockedNets {
			if blocked.Contains(ip) {
				return errors.New("webhook_url resolves to a private or reserved IP address")
			}
		}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
