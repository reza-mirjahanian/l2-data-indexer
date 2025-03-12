#!/bin/bash
include .env

# run the workers
run: ## calling the cmd to run the workers.
	@echo "\033[2m→ Running the project...\033[0m"
	@go run cmd/main.go

rund:
	@docker run -it -v "$(pwd)":/app my-go-app /bin/bash

# docker
docker:
	@docker build -t my-go-app .
	@docker run -it -v "$(pwd)":/app my-go-app /bin/bash
