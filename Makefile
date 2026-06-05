docker-test:
	docker-compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from test-runner

.PHONY: build test integration-test clean run docker-test setup-wal clean docker-build docker-run benchmark

all: build

build:
	go build -o mqueue ./main.go

run:
	./mqueue

test:
	go test ./...

integration-test:
	go test -tags=integration ./tests/...

setup-wal:
	mkdir -p ./wal/shard0 ./wal/shard1
	touch ./wal/shard0/wal.log ./wal/shard1/wal.log

clean:
	rm -f mqueue
	rm -rf ./wal

docker-build:
	docker-compose build

docker-run:
	docker-compose up -d

benchmark:
	docker-compose -f docker-compose.test.yml run --entrypoint "" -e BENCH_REQUESTS="2000" -e BENCH_CONCURRENCY="50" -e DATABASE_URLS="postgres://postgres:securepassword@postgres:5432/mqueue?sslmode=disable" -e REDIS_ADDRS="redis:6379" -e WAL_DIR="/app/wal" -e JWT_SECRET="super-secret-production-signing-key" --rm test-runner go run cmd/benchmark/main.go