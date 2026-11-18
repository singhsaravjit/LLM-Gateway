.PHONY: build test lint run-mock run-gateway up down smoke load

build:
	go build ./...

test:
	go test -race ./...

lint:
	gofmt -l . | (! grep .) && go vet ./...

run-mock:
	PORT=9000 go run ./cmd/mockllm

run-gateway:
	PROVIDER_URL=http://localhost:9000 go run ./cmd/gateway

up:
	docker compose up --build -d

down:
	docker compose down

smoke:
	curl -s -i localhost:8080/v1/chat/completions \
	  -H 'Authorization: Bearer test-key' -H 'Content-Type: application/json' \
	  -d '{"messages":[{"role":"user","content":"hello"}]}'

load:
	locust -f loadtest/locustfile.py --host http://localhost:8080
