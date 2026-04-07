CREATE TABLE postings (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transfer_id UUID NOT NULL REFERENCES transfers(id),
    account_id  BIGINT NOT NULL REFERENCES accounts(id),
    amount      BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON postings (account_id);

CREATE TABLE idempotency_keys (
    key           TEXT PRIMARY KEY,
    request_hash  TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('in_progress', 'done')),
    response_code INT,
    response_body JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
