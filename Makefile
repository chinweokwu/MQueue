docker-test:
	docker-compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from test-runner

.PHONY: build test integration-test clean run docker-test setup-wal clean docker-build docker-run

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