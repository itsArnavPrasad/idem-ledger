CREATE TABLE accounts (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        TEXT NOT NULL,
    currency    CHAR(3) NOT NULL,
    balance     BIGINT NOT NULL DEFAULT 0,
    version     BIGINT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (balance >= 0)
);

CREATE TABLE transfers (
    id              UUID PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('posted', 'failed')),
    amount          BIGINT NOT NULL CHECK (amount > 0),
    currency        CHAR(3) NOT NULL,
    from_account    BIGINT NOT NULL REFERENCES accounts(id),
    to_account      BIGINT NOT NULL REFERENCES accounts(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (from_account <> to_account),
    UNIQUE (idempotency_key)
);
