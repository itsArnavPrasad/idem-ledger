package ledger

import (
	"context"
	"errors"
	"testing"
)

// Execute validates the request before touching the DB, so these tests pass
// a nil pool — the guards return before pool.Begin is ever called.
func TestExecute_ValidationGuards(t *testing.T) {
	cases := []struct {
		name    string
		req     TransferRequest
		wantErr error
	}{
		{
			name:    "zero amount",
			req:     TransferRequest{FromAccount: 1, ToAccount: 2, Amount: 0, Currency: "INR"},
			wantErr: ErrInvalidAmount,
		},
		{
			name:    "negative amount",
			req:     TransferRequest{FromAccount: 1, ToAccount: 2, Amount: -50, Currency: "INR"},
			wantErr: ErrInvalidAmount,
		},
		{
			name:    "same from and to account",
			req:     TransferRequest{FromAccount: 5, ToAccount: 5, Amount: 100, Currency: "INR"},
			wantErr: ErrSameAccount,
		},
		{
			name:    "currency too short",
			req:     TransferRequest{FromAccount: 1, ToAccount: 2, Amount: 100, Currency: "US"},
			wantErr: ErrInvalidCurrency,
		},
		{
			name:    "currency too long",
			req:     TransferRequest{FromAccount: 1, ToAccount: 2, Amount: 100, Currency: "USDT"},
			wantErr: ErrInvalidCurrency,
		},
		{
			name:    "empty currency",
			req:     TransferRequest{FromAccount: 1, ToAccount: 2, Amount: 100, Currency: ""},
			wantErr: ErrInvalidCurrency,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := Execute(context.Background(), nil, tc.req)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("Execute() error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// Amount and currency checks fire in the same function (Execute), so the
// evaluation order determines which error surfaces first when multiple fields
// are invalid. This test documents and locks in that order.
func TestExecute_ValidationOrder(t *testing.T) {
	// Amount is checked before same-account.
	_, _, err := Execute(context.Background(), nil, TransferRequest{
		FromAccount: 1, ToAccount: 1, Amount: 0, Currency: "INR",
	})
	if !errors.Is(err, ErrInvalidAmount) {
		t.Errorf("expected ErrInvalidAmount to surface before ErrSameAccount, got %v", err)
	}

	// Same-account is checked before currency.
	_, _, err = Execute(context.Background(), nil, TransferRequest{
		FromAccount: 1, ToAccount: 1, Amount: 100, Currency: "X",
	})
	if !errors.Is(err, ErrSameAccount) {
		t.Errorf("expected ErrSameAccount to surface before ErrInvalidCurrency, got %v", err)
	}
}
