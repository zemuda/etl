# Makefile
.PHONY: build run stop clean logs shell test

# Variables
IMAGE_NAME = excel-parquet-pipeline
CONTAINER_NAME = excel-parquet-pipeline
VERSION ?= latest

# Build the Docker image
build:
	docker build -t $(IMAGE_NAME):$(VERSION) .

# Build with no cache
build-clean:
	docker build --no-cache -t $(IMAGE_NAME):$(VERSION) .

# Run with docker-compose
run:
	docker-compose up -d

# Run with file watcher
run-with-watcher:
	docker-compose --profile watcher up -d

# Run with monitoring
run-with-monitoring:
	docker-compose --profile monitoring up -d

# Run everything
run-all:
	docker-compose --profile watcher --profile monitoring up -d

# Stop all services
stop:
	docker-compose down

# View logs
logs:
	docker-compose logs -f

# View pipeline logs specifically
logs-pipeline:
	docker-compose logs -f excel-parquet-pipeline

# Shell into container
shell:
	docker-compose exec excel-parquet-pipeline /bin/bash

# Clean up everything
clean:
	docker-compose down -v
	docker system prune -f
	docker volume prune -f

# Run a one-time processing job
process:
	docker-compose run --rm excel-parquet-pipeline run --clean-orphaned

# Run in development mode
dev:
	docker-compose -f docker-compose.yml -f docker-compose.override.yml up -d

# Test the pipeline
test:
	docker-compose run --rm excel-parquet-pipeline --help

# Check status
status:
	docker-compose ps

# Update and restart
update: stop build run

# Show resource usage
stats:
	docker stats $(CONTAINER_NAME)