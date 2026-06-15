# RAGKB

多租户文档 RAG 平台：文档上传 -> 异步解析与向量化 -> 混合检索 -> Rerank -> LLM 生成带引用的回答。

完整设计见 [DESIGN.md](DESIGN.md)。

## 技术栈

- Go + Gin + GORM
- MySQL / Redis / Elasticsearch / Qdrant / MinIO
- Kafka / Apache Tika
- zap / OpenTelemetry / Prometheus

## 进程

| 进程 | 入口 | 职责 |
|---|---|---|
| api | `cmd/api` | HTTP 服务：认证、用户、文档上传与查询 |
| worker | `cmd/worker` | Kafka 消费：后续接入解析、分块、向量化、索引写入 |

## 快速开始

前置依赖：Go 1.26+、Docker。

```bash
# 1. 启动本地依赖
docker compose -f deploy/docker-compose.yaml up -d

# 2. 初始化数据库
cmd /c "docker exec -i ragkb-mysql mysql -uroot -p123456 ragkb < migrations\init.sql"

# 3. 准备本地配置
cp configs/config.example.yaml configs/config.yaml

# 4. 编辑 configs/config.yaml
# 至少确认 mysql.dsn 和 jwt.secret 可用。

# 5. 启动 API
go run ./cmd/api -config configs/config.yaml

# 6. 另开终端启动 worker
go run ./cmd/worker -config configs/config.yaml
```

## 配置约定

- `configs/config.example.yaml` 是可提交的示例配置。
- `configs/config.yaml` 是本地真实配置，已被 `.gitignore` 忽略。
- 配置只从 YAML 文件读取，不再读取 `RAGKB_*` 环境变量。

## 常用命令

```bash
make help
make build
make test
make up
make down
```

## 当前目录

```text
cmd/
  api/                    HTTP 服务入口
  worker/                 Kafka worker 入口
configs/                  YAML 配置
deploy/                   Docker Compose
internal/
  bootstrap/              进程依赖初始化与装配
  config/                 配置加载与校验
  domain/                 领域模型、仓储接口、领域错误
  handler/                HTTP handler
  indexing/               Worker 索引维护：抽取、分块、embedding、索引写入、删除清理
  infra/                  MySQL / Redis / ES / Qdrant / MinIO / Kafka / JWT
  observability/          日志与后续可观测性
  response/               HTTP 响应包装
  server/                 Gin 引擎与中间件
  service/                应用服务编排
migrations/               数据库初始化 SQL
```
