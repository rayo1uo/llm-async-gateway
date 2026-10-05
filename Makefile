.PHONY: build test race vet lint ci demo up down

build:
	mkdir -p bin
	go build -o bin/gateway ./cmd/gateway
	go build -o bin/gateway-api ./cmd/gateway-api
	go build -o bin/batch-controller ./cmd/batch-controller
	go build -o bin/dispatcher ./cmd/dispatcher
	go build -o bin/mockupstream ./cmd/mockupstream

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

lint:
	golangci-lint run ./...

ci: vet lint race

demo: build
	bash scripts/demo.sh

up:
	docker compose up --build

down:
	docker compose down
