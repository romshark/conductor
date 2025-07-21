.PHONY: all vulncheck fmtcheck test

lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run ./...

vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

fmtcheck:
	@unformatted=$$(go run mvdan.cc/gofumpt@latest -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "Files not gofumpt formatted:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

test: fmtcheck lint
	docker-compose up -d
	go test -race -coverpkg=./... -v ./...
	docker-compose down -v
