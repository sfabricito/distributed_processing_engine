SHELL := /bin/bash
APP_IMAGE ?= mini-spark:latest
COMPOSE ?= $(shell if command -v docker-compose >/dev/null 2>&1; then echo docker-compose; else echo "docker compose"; fi)

.PHONY: build build-release docker-build test test-shuffle test-all demo test-cli

build:
	cargo build --bin distributed_processing_engine --bin mini-spark-cli

build-release:
	cargo build --release --bin distributed_processing_engine --bin mini-spark-cli

docker-build:
	docker build -t $(APP_IMAGE) .

test:
	cargo test

test-shuffle:
	cargo test shuffle

test-all: test

demo:
	COMPOSE="$(COMPOSE)" ./scripts/demo.sh

test-cli:
	COMPOSE="$(COMPOSE)" ./scripts/test_cli.sh

test-cli-live:
	COMPOSE="$(COMPOSE)" ./scripts/test_cli_live.sh
