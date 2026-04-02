DATABASE_URL ?= postgres://idem:idem@localhost:5432/idemledger?sslmode=disable

.PHONY: db-up db-down migrate-up migrate-down run build test

db-up:
	docker compose up -d

db-down:
	docker compose down

migrate-up:
	migrate -path ./migrations -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path ./migrations -database "$(DATABASE_URL)" down

run:
	DATABASE_URL=$(DATABASE_URL) go run ./cmd/api

build:
	go build -o bin/idem-ledger ./cmd/api

test:
	go test ./...
