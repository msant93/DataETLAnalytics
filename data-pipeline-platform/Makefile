# Common developer + CI commands. Run `make help` for the list.
.PHONY: help setup install test lint run run-incremental validate docker clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  %-16s %s\n", $$1, $$2}'

setup: ## One-command environment setup (venv + deps + validate)
	./setup.sh

install: ## Install dev dependencies into the current environment
	pip install -r requirements-dev.txt

test: ## Run the test suite
	pytest

lint: ## Static checks
	ruff check .

run: ## Run the batch pipeline (uses config profile)
	python -m etl.cli run

run-incremental: ## Run the incremental (merge) demo
	python -m etl.cli run --incremental

validate: ## Environment smoke test
	python -m etl.cli validate

docker: ## Build the container image
	docker build -t local-data-pipeline:latest .

clean: ## Remove generated artifacts
	rm -rf output/*.duckdb output/*.duckdb.wal output/*.csv \
		__pycache__ */__pycache__ .pytest_cache \
		data/orders_source.csv notebooks/.ipynb_checkpoints
