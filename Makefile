.PHONY: build test lint check

build:
	go build -buildvcs=false ./cmd/...

test:
	go test -cover ./...

lint:
	golangci-lint run

check: build test lint
