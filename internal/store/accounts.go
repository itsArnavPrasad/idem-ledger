package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")

type Account struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Currency  string    `json:"currency"`
	Balance   int64     `json:"balance"`
	CreatedAt time.Time `json:"created_at"`
}

func CreateAccount(ctx context.Context, db *pgxpool.Pool, name, currency string) (Account, error) {
	var a Account
	err := db.QueryRow(ctx,
		`INSERT INTO accounts (name, currency) VALUES ($1, $2)
		 RETURNING id, name, currency, balance, created_at`,
		name, currency,
	).Scan(&a.ID, &a.Name, &a.Currency, &a.Balance, &a.CreatedAt)
	return a, err
}

func GetAccount(ctx context.Context, db *pgxpool.Pool, id int64) (Account, error) {
	var a Account
	err := db.QueryRow(ctx,
		`SELECT id, name, currency, balance, created_at FROM accounts WHERE id = $1`,
		id,
	).Scan(&a.ID, &a.Name, &a.Currency, &a.Balance, &a.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, ErrNotFound
	}
	return a, err
}
