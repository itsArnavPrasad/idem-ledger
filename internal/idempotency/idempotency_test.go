package idempotency

import (
	"testing"
)

func TestHashRequest_KeyOrderNormalization(t *testing.T) {
	// Two payloads that are logically identical but have different JSON key order.
	// A client that serializes its struct fields in a different order on retry
	// must get the same hash — otherwise they receive 422 ErrDuplicateRequest
	// for a legitimate retry, which would be very hard to debug.
	a := HashRequest([]byte(`{"amount":100,"currency":"USD"}`))
	b := HashRequest([]byte(`{"currency":"USD","amount":100}`))
	if a != b {
		t.Errorf("same content, different key order: got different hashes\n  a=%s\n  b=%s", a, b)
	}
}

func TestHashRequest_WhitespaceNormalization(t *testing.T) {
	compact := HashRequest([]byte(`{"amount":100}`))
	spaced := HashRequest([]byte(`{ "amount" : 100 }`))
	if compact != spaced {
		t.Errorf("same content, different whitespace: got different hashes\n  compact=%s\n  spaced=%s", compact, spaced)
	}
}

func TestHashRequest_DifferentContent(t *testing.T) {
	h100 := HashRequest([]byte(`{"amount":100}`))
	h200 := HashRequest([]byte(`{"amount":200}`))
	if h100 == h200 {
		t.Errorf("different amounts produced the same hash: %s", h100)
	}
}

func TestHashRequest_NonJSONFallsBackToRawBytes(t *testing.T) {
	// Non-JSON bodies (e.g. plain text) should still hash — they use the raw bytes.
	// The same raw bytes must always produce the same hash.
	h1 := HashRequest([]byte(`not json at all`))
	h2 := HashRequest([]byte(`not json at all`))
	if h1 != h2 {
		t.Errorf("same non-JSON body produced different hashes: %s vs %s", h1, h2)
	}

	// Different non-JSON bodies must differ.
	h3 := HashRequest([]byte(`different`))
	if h1 == h3 {
		t.Errorf("different non-JSON bodies produced the same hash: %s", h1)
	}
}

func TestHashRequest_EmptyBody(t *testing.T) {
	h := HashRequest([]byte{})
	if h == "" {
		t.Error("empty body produced an empty hash")
	}
	// Same empty body always returns the same hash.
	if HashRequest([]byte{}) != h {
		t.Error("empty body is not deterministic")
	}
}
