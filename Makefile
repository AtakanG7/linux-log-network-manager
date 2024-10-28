.PHONY: build run test clean receiver

build:
	CGO_ENABLED=0 go build -o bin/agent cmd/main.go
	CGO_ENABLED=0 go build -o bin/receiver cmd/receiver/main.go

run:
	go run cmd/main.go

receiver:
	go run cmd/receiver/main.go

test:
	go test -v ./...

clean:
	rm -rf bin/