.PHONY: build test lint check migrate perf compose-build compose-up compose-stop compose-down compose-test

build:
	go build -buildvcs=false ./cmd/...

test:
	go test -cover ./...

lint:
	golangci-lint run

check: build test lint

migrate:
	go run ./cmd/migrate

perf:
	go run ./cmd/perf --report perf-report.json

compose-build:
	docker compose build

compose-up:
	docker compose up --build --detach --wait

compose-stop:
	docker compose stop

compose-down:
	docker compose down

compose-test:
	docker compose config --quiet
	go test -cover ./...
