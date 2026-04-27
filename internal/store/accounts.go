package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")

type Posting struct {
	ID         int64     `json:"id"`
	TransferID string    `json:"transfer_id"`
	Amount     int64     `json:"amount"`
	CreatedAt  time.Time `json:"created_at"`
}

// GetPostings returns up to limit postings for the account, ordered by id desc
// (newest first). afterID is the cursor: pass 0 for the first page, then the
// smallest id from the previous page to get the next page.
func GetPostings(ctx context.Context, db *pgxpool.Pool, accountID, afterID int64, limit int) ([]Posting, error) {
	query := `SELECT id, transfer_id, amount, created_at
	          FROM postings WHERE account_id = $1`
	args := []any{accountID}

	if afterID > 0 {
		query += ` AND id < $2 ORDER BY id DESC LIMIT $3`
		args = append(args, afterID, limit)
	} else {
		query += ` ORDER BY id DESC LIMIT $2`
		args = append(args, limit)
	}

	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var postings []Posting
	for rows.Next() {
		var p Posting
		if err := rows.Scan(&p.ID, &p.TransferID, &p.Amount, &p.CreatedAt); err != nil {
			return nil, err
		}
		postings = append(postings, p)
	}
	return postings, rows.Err()
}

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
