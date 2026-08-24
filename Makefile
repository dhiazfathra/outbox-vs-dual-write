.PHONY: bench test lint clean up down

bench: ## run every arm and write results/
	./run-bench.sh

test:
	go test ./...

lint:
	golangci-lint run

up:
	docker compose up -d

down:
	docker compose down -v

clean:
	rm -rf bin results/*.json results/*.log
