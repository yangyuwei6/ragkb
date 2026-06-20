# RAGKB Implementation Notes

> 本文档记录项目的实际实现决策、当前进度和后续待讨论事项。
> 当项目结构、关键技术取舍或模块状态发生变化时，同步更新本文档。

## 1. 文档定位

本文档只记录当前已经达成共识的实际实现方式，不追踪历史方案差异。

记录范围：

- 当前架构和目录约定。
- 当前领域边界。
- 当前已经实现的模块。
- 当前尚未实现的模块。
- 后续需要讨论或确认的问题。

## 2. 当前架构约定

项目采用 Go 单 module + DDD 分层方式组织代码。

```text
cmd/
  api/                    HTTP API 进程入口
  worker/                 后台 worker 进程入口
configs/                  配置文件示例与本地配置
deploy/                   本地依赖和部署配置
internal/
  bootstrap/              进程依赖装配
  config/                 配置加载和配置结构
  domain/                 领域模型、领域仓储接口、领域错误
  handler/                HTTP handler 和 DTO
  infra/                  外部依赖适配：MySQL、Redis、Kafka、MinIO、ES、Qdrant 等
  observability/          日志和后续可观测性能力
  response/               HTTP 统一响应结构
  server/                 Gin 路由和中间件
  service/                应用服务，用例编排
migrations/               数据库初始化脚本
```

DDD 分层边界：

- `domain` 只表达业务概念，不依赖 HTTP、Gin、MySQL、Redis、ES、Qdrant 等具体技术。
- `service` 负责编排用例，可以组合多个领域对象和基础设施接口。
- `handler` 负责 HTTP 入参、出参、认证上下文和错误响应。
- `infra` 负责外部系统的具体实现。
- `bootstrap` 负责把具体实现装配到应用服务和 handler 中。

## 3. 领域边界

当前已确认的领域包：

```text
internal/domain/user
internal/domain/document
internal/domain/chunk
internal/domain/conversation
```

含义：

- `user`：用户、租户、认证相关的领域对象和仓储接口。
- `document`：文档元数据、上传状态、摄取状态、文档事件。
- `chunk`：文档切分后的最小检索单元。
- `conversation`：会话和消息，后续承接 RAG 问答历史。

已做出的调整：

- `Chunk` 和 `Chunk Repository` 已从 `domain/document` 拆出到 `domain/chunk`。
- `document` 领域只保留文档本体、文档事件、文档仓储接口。

## 4. 应用服务约定

当前已确认的应用服务目录：

```text
internal/service/auth
internal/service/user
internal/service/document
internal/service/ingest
internal/service/search
internal/service/chat
```

职责：

- `auth`：注册、登录、刷新 token、退出登录。
- `user`：当前用户信息、租户信息。
- `document`：文档上传、分片合并、文档查询、文档删除。
- `ingest`：后台摄取流程，后续负责解析文件、切分 chunk、embedding、写入索引。
- `search`：在线检索流程，后续负责权限过滤、BM25/向量召回、RRF、rerank。
- `chat`：RAG 问答流程，后续负责读取会话、检索上下文、调用 LLM、保存消息。

注意：

- 不新增泛化的 `internal/indexing`、`internal/retrieval` 作为顶层技术目录。
- RAG 技术步骤放到应用服务中编排，具体外部依赖放到 `infra`。

## 5. 数据库表边界

当前 SQL 表大致对应以下业务边界：

```text
users / tenants / user_tenants
  -> user 领域

documents
  -> document 领域

chunks
  -> chunk 领域

conversations / messages
  -> conversation 领域
```

当前判断：

- 现有表结构可以支撑 RAG 主链路。
- `documents` 保存文档元数据和摄取状态。
- `chunks` 保存检索索引的数据真源。
- ES/Qdrant 只作为检索索引，不作为业务真源。
- `conversations/messages` 后续用于保存问答历史和引用信息。

待讨论：

- `tenant_tag` 是否长期作为稳定业务键使用，还是未来补充不可变 `tenant_id`。
- `messages.citations` 的 JSON 结构需要在实现 chat 服务前明确。

## 6. 当前已实现

- API 进程入口和优雅关闭。
- 配置加载。
- zap 日志初始化。
- Gin 路由和 JWT 中间件。
- 用户注册、登录、刷新 token、退出登录。
- 当前用户和租户查询。
- 文档分片上传、合并、秒传判断、断点续传判断。
- 文档列表、详情、软删除。
- 上传完成后发布文档摄取事件。
- 删除文档后发布索引清理事件。
- MySQL、Redis、MinIO、Kafka 等基础设施适配的初步实现。
- `domain/chunk` 已独立出来。
- `service/ingest`、`service/search`、`service/chat` 已建立骨架。

## 7. 当前未实现

- worker 进程实际启动逻辑。
- Kafka 消费文档摄取事件。
- Tika 文件解析。
- 文本切分策略。
- embedding 批量调用和重试。
- chunk 写入 MySQL。
- ES BM25 索引写入和删除。
- Qdrant 向量索引写入和删除。
- 混合检索。
- RRF 融合。
- rerank 调用和降级。
- chat 问答接口。
- LLM 流式生成。
- 引用拼装和消息持久化。
- eval 评估 CLI。
- OpenTelemetry 和 Prometheus 指标。

## 8. 待确认事项

- `tenant_tag` 是否长期作为稳定业务键使用，还是未来补充不可变 `tenant_id`。
- `messages.citations` 的 JSON 结构需要在实现 chat 服务前明确。
  暂定用途是保存回答引用到的文档、chunk 序号和原文位置。

## 9. 注释约定

- 代码注释默认使用中文，源码统一按 UTF-8 保存。
- 只在必要位置加注释：领域对象、接口、用例入口、命令对象、非显而易见的业务规则。
- 不给简单 getter、普通赋值和显而易见的代码加重复注释。

## 10. 下一步建议

优先顺序：

1. 实现 `worker` 启动和 Kafka 消费框架。
2. 实现 `service/ingest` 的主流程骨架。
3. 补齐 `infra/mysql` 的 chunk repository。
4. 接入 Tika 解析和最小可用切分逻辑。
5. 将 chunk 落 MySQL，再逐步接入 embedding、ES、Qdrant。
