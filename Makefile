.PHONY: help deps up down logs build run-api run-worker tidy fmt vet test clean

# 默认配置文件路径
CONFIG ?= configs/config.yaml

help: ## 显示可用命令
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

deps: ## 拉取依赖
	go mod download

up: ## 启动全套依赖容器
	docker compose -f deploy/docker-compose.yaml up -d

down: ## 停止并移除依赖容器
	docker compose -f deploy/docker-compose.yaml down

logs: ## 跟踪依赖容器日志
	docker compose -f deploy/docker-compose.yaml logs -f

build: ## 编译 api 和 worker 到 bin/
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker

run-api: ## 启动 API 服务
	go run ./cmd/api -config $(CONFIG)

run-worker: ## 启动 worker
	go run ./cmd/worker -config $(CONFIG)

tidy: ## 整理 go.mod
	go mod tidy

fmt: ## 格式化代码
	go fmt ./...

vet: ## 静态检查
	go vet ./...

test: ## 运行测试
	go test ./...

clean: ## 清理编译产物
	rm -rf bin/
