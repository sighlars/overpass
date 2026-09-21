.PHONY: build test vet run fmt tidy docker

build:
	go build -o bin/overpass ./cmd/overpass

test:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -l .

run:
	go run ./cmd/overpass

tidy:
	go mod tidy

docker:
	docker build -t overpass:latest .
