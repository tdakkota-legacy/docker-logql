test:
	@./go.test.sh
.PHONY: test

coverage:
	@./go.coverage.sh
.PHONY: coverage

tidy:
	go mod tidy
.PHONY: tidy

generate:
	go generate ./...

install:
	mkdir -pv ~/.docker/cli-plugins/
	go build -v -o ~/.docker/cli-plugins/docker-logql ./cmd/docker-logql/ 
.PHONY: install