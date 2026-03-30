BINARY  := clicks3
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-s -w -X main.version=$(VERSION)"

.PHONY: build install clean test lint docker

build:
	go build $(LDFLAGS) -o $(BINARY) .

install:
	go install $(LDFLAGS) .

clean:
	rm -f $(BINARY)
	rm -f clicks3-linux-amd64 clicks3-linux-arm64 clicks3-darwin-amd64 clicks3-darwin-arm64

test:
	go test -v -race ./...

lint:
	golangci-lint run

# Cross-compilation for deploying on benchmark nodes
release: clean
	GOOS=linux  GOARCH=amd64 go build $(LDFLAGS) -o clicks3-linux-amd64 .
	GOOS=linux  GOARCH=arm64 go build $(LDFLAGS) -o clicks3-linux-arm64 .
	GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o clicks3-darwin-amd64 .
	GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o clicks3-darwin-arm64 .

docker:
	docker build -t clicks3:$(VERSION) .

# Quick test against local MinIO
quicktest: build
	./$(BINARY) \
		--endpoint http://localhost:9000 \
		--access-key minioadmin \
		--secret-key minioadmin \
		--bucket clicks3-test \
		--duration 1m \
		--scenarios failures \
		--output report.json
