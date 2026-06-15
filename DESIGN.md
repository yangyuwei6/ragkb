# RAGKB — 多租户文档 RAG 平台 · 详细设计文档

> 一句话定位：上传文档 → 异步解析向量化 → 混合检索(BM25 + 向量) + RRF 融合 + Rerank 精排 → LLM 流式生成带引用的答案。
>
> 本文档是后续编码的蓝图，也是面试时讲解项目的脚本。每个设计决策都附带"为什么"，因为面试官问的永远是 why 而不是 what。

---

## 0. 文档导航

- [1. 项目定位与简历价值](#1-项目定位与简历价值)
- [2. 整体架构](#2-整体架构)
- [3. 技术选型与取舍](#3-技术选型与取舍-面试核心)
- [4. 数据模型](#4-数据模型)
- [5. 摄取管线（Ingestion）](#5-摄取管线ingestion)
- [6. 检索层（Retrieval）](#6-检索层retrieval)
- [7. 生成层（Generation）](#7-生成层generation)
- [8. 多租户与权限](#8-多租户与权限)
- [9. 可观测性与可靠性](#9-可观测性与可靠性)
- [10. RAG 评估体系](#10-rag-评估体系)
- [11. API 设计](#11-api-设计)
- [12. 目录结构](#12-目录结构)
- [13. 分阶段实施路线](#13-分阶段实施路线)
- [14. 简历技术点清单](#14-简历技术点清单与面试问答准备)

---

## 1. 项目定位与简历价值

### 1.1 它解决什么问题

企业内部有大量文档（PDF/Word/Excel/PPT/Markdown），员工想用自然语言提问并得到**有出处、不瞎编**的答案。这就是企业知识库 RAG 的典型场景。

### 1.2 工程级 RAG 的设计要点

一个 RAG demo 和一个工程级 RAG 系统的区别，全在以下这些"非功能性"环节上。本项目刻意把它们都做全：

| 维度 | demo 级做法 | 本项目 |
|---|---|---|
| 文档摄取 | 同步阻塞、单文件 | Kafka 异步、分片上传、断点续传、秒传 |
| 分块 | 定长暴力切 | 结构感知 + token 级切分 + 重叠 |
| 向量化 | 逐条串行调 API | 批量 + 并发 + 限流 + 重试 |
| 检索 | 单路向量 | BM25 + 向量双路 + RRF 融合 + Rerank 精排 |
| 生成 | 拼 prompt 一次性返回 | 流式 SSE + 引用标注 + grounding 防幻觉 |
| 可靠性 | 失败即丢 | 消费重试 + 死信队列 + 幂等 |
| 多租户 | 无 | user/org/public 三级权限过滤 |
| 可观测 | print 日志 | 结构化日志 + Trace + Metrics |
| 质量 | 无 | RAG 评估体系（recall@k / MRR / 忠实度） |
| 测试 | 无 | 单测 + testcontainers 集成测试 + CI |

### 1.3 非目标（Non-Goals）

明确"不做什么"和"做什么"同样重要——它体现 scope 控制能力，也避免面试官拿这些来质疑"为什么没做"。本项目刻意不做：

- **不做扫描件 OCR**：只处理有文本层的文档。扫描件/纯图片走"提取为空"失败路径，OCR 留作未来扩展。
- **不做文档在线编辑/协作**：文档是只读知识源，上传后不可在线改，更新靠重新上传（覆盖式 re-ingest）。
- **不做 multi-hop 复杂推理**：单次问答是"检索相关片段 → 生成"，不做需要多步跳转、Agent 规划的复杂推理（如"对比 A、B 两文档第 3 章再推演")。
- **不做模型训练/微调**：embedding、rerank、LLM 都用现成模型/API，不自训。项目重点是检索与工程，不是模型研究。
- **不追求真正的 exactly-once**：用 at-least-once + 幂等达到等效结果（见 5.7），不上 Kafka 事务等重型方案。
- **不做前端**：项目聚焦后端服务与 RAG 链路，前端仅在需要时做最简验证页。

> **面试点**：能主动划清边界，说明你懂"工程是有限资源下的取舍"，而不是什么都想做、什么都做不深。

---

## 2. 整体架构

### 2.1 进程拆分

三个独立可执行程序，对应三种角色，可独立部署与扩缩容：

- **`cmd/api`** — HTTP 服务。处理上传、检索、问答、用户管理。无状态，可水平扩展。
- **`cmd/worker`** — Kafka 消费者。执行摄取管线（解析 → 分块 → 向量化 → 双写索引）。CPU/IO 密集，独立扩容。
- **`cmd/eval`** — 离线评估 CLI。跑评测集，输出 recall@k / MRR / 忠实度等指标。

> **关于代码组织**：三个进程同属**一个 Go module、一个仓库**（monorepo + 多 `main` 入口），共享 `internal/` 下的领域模型、存储客户端、配置等代码，避免重复。不是三个独立微服务/独立仓库——那样会引入跨服务接口同步、版本对齐的额外成本，对当前规模没必要。各 `cmd/xxx/main.go` 只做依赖装配与启动，业务逻辑全在 `internal/`。

> **面试点**：为什么 api 和 worker 拆开？因为摄取（向量化、文档解析）是重 CPU/IO 的慢任务，如果和在线请求挤在同一进程，会拖慢 P99 延迟、抢占资源，且无法独立扩容。拆开后 worker 可以根据 Kafka 积压量单独加副本。而它们共享一个 module，是因为拆的是「部署单元」而非「代码库」——这是单体应用拆分进程的常见做法，不必一上来就上微服务。

### 2.2 数据流全景

```
┌─────────── 写入路径（异步摄取） ───────────┐

 Client ──分片上传──▶ API ──存分片──▶ MinIO
                       │
                  合并完成后
                       │ 发送 FileIngest 消息
                       ▼
                    Kafka (topic: doc.ingest)
                       │
                       ▼
 Worker ◀──消费──┐
   │ 1. 拉取合并文件 (MinIO)
   │ 2. Tika 抽取文本
   │ 3. 智能分块 (结构感知 + token切分)
   │ 4. 批量 Embedding (并发+重试)
   │ 5. 双写：
   │      ├─▶ Elasticsearch  (BM25 全文索引)
   │      └─▶ Qdrant         (向量索引)
   │ 失败 ──▶ 重试 N 次 ──▶ 仍失败 ──▶ DLQ (doc.ingest.dlq)
   └────────────────────────────────────────┘

┌─────────── 读取路径（在线问答） ───────────┐

 Client ──问题──▶ API
   │ 1. (可选) 查询改写 / HyDE
   │ 2. 问题向量化 (Embedding)
   │ 3. 并行召回：
   │      ├─▶ Elasticsearch BM25  → 排序列表 A
   │      └─▶ Qdrant 向量 KNN     → 排序列表 B
   │      (两路都带多租户权限过滤)
   │ 4. RRF 融合 A + B → 统一候选集
   │ 5. Rerank 精排 (cross-encoder / rerank API) → top-k
   │ 6. 拼装 Context + 引用元数据
   │ 7. LLM 流式生成 ──SSE──▶ Client
   │      (答案中带 [1][2] 引用标记)
   └────────────────────────────────────────┘
```

### 2.3 组件清单

| 组件 | 角色 | 替代方案 / 备注 |
|---|---|---|
| MySQL | 业务元数据（用户、文件记录、分块记录、会话） | 关系型，事务强一致 |
| Redis | 上传进度标记、会话记忆、限流、缓存 | |
| Elasticsearch | BM25 全文检索 + 中文分词(IK) | 只做关键词，不再做向量 |
| Qdrant | 向量检索（HNSW + 量化 + payload 过滤） | 专用向量库 |
| Kafka | 摄取任务异步解耦 + 重试 + DLQ | |
| MinIO | 对象存储（原始文件分片 + 合并文件） | S3 兼容 |
| Tika | 文档文本抽取（PDF/Office/etc.） | 独立服务 |
| LLM API | 答案生成（流式） | OpenAI 兼容接口 |
| Embedding API | 文本向量化 | OpenAI 兼容接口 |
| Rerank | 候选精排 | rerank API 或本地 cross-encoder |

---

## 3. 技术选型与取舍 (面试核心)

> 这一节是整个文档最值钱的部分。面试官不在乎你用了什么，在乎你**为什么这么选、放弃了什么**。每个决策都要能背出来。

### 3.1 为什么 ES + Qdrant 拆开，而不是只用 ES 的 dense_vector？

**事实**：ES 8.x 的 `dense_vector` 字段本身就能同时做 BM25 + KNN 向量检索，单库即可完成混合检索。技术上完全够用。

**我们仍然拆开的理由**：

1. **职责单一 + 各自最优**。ES 是为倒排索引和全文检索而生，BM25 是它的看家本领；Qdrant 是为 ANN 向量检索而生，HNSW 索引、标量/乘积量化、payload 过滤都是一等公民。各用所长。
2. **向量规模与成本**。当 chunk 数量到百万级，向量占用内存巨大。Qdrant 支持标量量化（float32→int8，省 75% 内存）和 on-disk 存储，ES 在这方面较弱。
3. **可独立扩缩容**。向量检索和全文检索负载特征不同，拆开后能分别扩容。

**代价（必须诚实承认）**：

- 引入**双写一致性**问题：同一 chunk 要同时进 ES 和 Qdrant，可能一边成功一边失败。
- 多一个有状态组件，运维复杂度上升。

**我们如何应对双写一致性**（这是面试高频追问）：

- **单一写入方**：只有 worker 通过 Kafka 消费来写索引，没有其他写入路径，避免并发写竞争。
- **幂等 upsert**：两边都用 `{file_id}_{chunk_index}` 作为文档 ID，重复消费覆盖而非新增。
- **同一消息内双写 + 失败整体重试**：worker 在一条消息的处理中先写 Qdrant 再写 ES，任一失败则整条消息进入重试，最终进 DLQ。因为是幂等的，重试不会产生脏数据。
- **进阶（文档中标注为可选增强）**：transactional outbox 模式 —— 先把"待索引"写进 MySQL 的 outbox 表（与业务同事务），再由独立组件读 outbox 投递，保证至少一次。

> **一句话总结给面试官**：我们用"单写入方 + 幂等 upsert + 失败整体重试"把双写一致性退化成最终一致性问题，代价可控，换来检索层职责清晰和向量库的专业能力。

### 3.2 为什么需要 RRF 融合，不能直接把两路分数相加？

**问题**：BM25 分数和向量余弦相似度**量纲完全不同**。BM25 可能是 0~30 的无界分数，余弦相似度是 0~1。直接相加，BM25 会碾压向量分数，融合失去意义。归一化（min-max）又对异常值敏感、跨查询不稳定。

**RRF（Reciprocal Rank Fusion）的做法**：不看分数，只看**排名**。

```
RRF_score(doc) = Σ  1 / (k + rank_i(doc))
                 i∈各路检索结果
```

其中 `rank_i(doc)` 是 doc 在第 i 路结果中的排名（从 1 开始），`k` 是平滑常数（论文经验值 60）。

- 一个文档如果在 BM25 和向量两路里都排名靠前，RRF 分数就高。
- 完全规避了量纲问题，因为只用排名。
- 实现极简，效果稳定，是工业界事实标准（ES、Weaviate 都内置）。

> **面试点**：能讲清"为什么不能直接加分数"和"RRF 用排名而非分数"，就已经超过大多数候选人。

### 3.3 为什么召回之后还要 Rerank？

**召回阶段**（BM25 + 向量）追求的是**高召回率**：宁可多召回一些，不要漏。所以会取 top-50 甚至 top-100。但这些候选里有不少是"相关但不精准"的。

**Rerank 阶段**用更强但更慢的模型（cross-encoder，把 query 和每个候选**拼在一起**算相关性，而不是像 embedding 那样各自独立编码）对候选重排，取最终 top-3~5 喂给 LLM。

**为什么不直接用 cross-encoder 做召回？** 因为它要对每个候选都跑一次 query-doc 联合编码，无法预先建索引，全量算开销爆炸。所以是"双塔召回（快、可索引）→ 交叉编码精排（准、量小）"的经典两阶段架构。

> **面试点**：双塔 vs 交叉编码的区别、召回与精排的分工，是检索系统的核心 know-how。

### 3.4 为什么用 Kafka 而不是直接同步处理 / 或用 Redis 队列？

- **同步处理不可行**：一个大 PDF 解析 + 分块 + 几百个 chunk 的向量化要几十秒到几分钟，HTTP 请求扛不住。
- **vs Redis List/Stream**：Kafka 提供持久化、消费位移、消费组、天然的重试与 DLQ 模式、更高吞吐。摄取任务"宁可慢、不可丢"，Kafka 的持久化保证更合适。
- **DLQ（死信队列）**：处理失败重试 N 次仍失败的消息进入 `doc.ingest.dlq`，人工排查后可重放，避免毒丸消息阻塞整个消费组。

### 3.5 为什么用 Tika 抽取文本？

文档格式五花八门（PDF 还分有无文本层、Office 老新格式、扫描件）。Apache Tika 是成熟的统一抽取方案，支持上千种格式，独立服务部署，避免在 Go 进程里塞一堆格式解析库。扫描件 OCR 可后续接入。

### 3.6 选型决策速查表

| 决策 | 选择 | 主要理由 | 放弃的方案 |
|---|---|---|---|
| 向量存储 | Qdrant（独立） | 量化省内存、HNSW、payload 过滤 | ES dense_vector（藏细节）、pgvector（规模受限） |
| 全文检索 | Elasticsearch + IK | BM25 看家、中文分词成熟 | 只靠向量（关键词/数字/专名召回差） |
| 融合 | RRF | 免归一化、用排名、工业标准 | 加权求和（量纲不一致） |
| 精排 | Rerank/cross-encoder | 召回准确率提升大 | 不做（context 噪声多，幻觉增加） |
| 异步 | Kafka + DLQ | 持久化、消费组、可重放 | 同步（超时）、Redis 队列（可靠性弱） |
| 文本抽取 | Apache Tika | 格式覆盖广、独立服务 | 各语言库拼凑（维护成本高） |
| 生成 | OpenAI 兼容流式 | 生态通用、可换模型 | 锁定单一厂商 |

### 3.7 为什么用 Go 自研，而不是 LangChain / LlamaIndex？

这是 RAG 项目**几乎必被问**的问题，必须有准备好的答案。

LangChain/LlamaIndex 适合**快速搭原型**：几十行就能跑通一个 RAG demo。但它们不适合做这个项目的目标——一个**生产级、可面试深挖**的系统。理由：

1. **语言与部署**：它们是 Python 生态。高并发在线服务用 Go，受益于原生并发（goroutine 并行召回、流式）、低内存、单二进制部署、无 GIL。后端服务岗位本身也更看重 Go 工程能力。
2. **可控性与可观测性**：框架把检索、融合、prompt 拼装层层封装，链路黑盒化。本项目要在每一环埋 trace/metrics、要精细控制 rerank 超时降级、要定制 RRF 与权限下推——这些在框架里要么做不了，要么得跟它的抽象搏斗。
3. **面试价值**：自己实现 RRF、两阶段检索、双写一致性、流式 SSE，才证明你**理解 RAG 每一环的原理**。用框架则相当于"我会调 API"，恰恰是面试想穿透的那层。框架隐藏的复杂度，正是面试官想考的复杂度。
4. **依赖轻量**：不背负框架庞大的依赖树和版本耦合，长期可维护性更好。

> **一句话总结给面试官**："框架适合验证想法，不适合作为我要深度掌控和讲解的项目。我自研是为了性能可控、可观测性可控，以及真正吃透 RAG 的每一个环节——而这正是这个项目对我的价值。"
>
> 注意分寸：不要贬低框架（面试官可能正在用），强调的是"针对本项目目标的取舍"，不是"框架不好"。

## 4. 数据模型

### 4.1 MySQL（业务元数据，强一致）

```sql
-- 用户
CREATE TABLE users (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    username    VARCHAR(64)  NOT NULL UNIQUE,
    password    VARCHAR(255) NOT NULL,              -- bcrypt
    role        ENUM('USER','ADMIN') NOT NULL DEFAULT 'USER',
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 租户/组织（多租户隔离单元）
CREATE TABLE tenants (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    tag         VARCHAR(64) NOT NULL UNIQUE,         -- 例如 PRIVATE_alice / TEAM_infra
    name        VARCHAR(128) NOT NULL,
    parent_tag  VARCHAR(64) DEFAULT NULL,            -- 支持组织树
    created_by  BIGINT NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 用户↔租户多对多
CREATE TABLE user_tenants (
    user_id     BIGINT NOT NULL,
    tenant_tag  VARCHAR(64) NOT NULL,
    is_primary  TINYINT(1) NOT NULL DEFAULT 0,       -- 默认上传归属
    PRIMARY KEY (user_id, tenant_tag),
    INDEX idx_tenant (tenant_tag)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 文件（一个逻辑文件，按 MD5 去重 → 秒传）
CREATE TABLE documents (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    file_md5    VARCHAR(32) NOT NULL,
    file_name   VARCHAR(255) NOT NULL,
    file_ext    VARCHAR(16) NOT NULL,
    total_size  BIGINT NOT NULL,
    -- 上传与处理状态机：见 5.6
    upload_status  TINYINT NOT NULL DEFAULT 0,  -- 0=上传中 1=已合并
    ingest_status  TINYINT NOT NULL DEFAULT 0,  -- 0=待处理 1=处理中 2=完成 3=失败
    chunk_count    INT NOT NULL DEFAULT 0,
    embedding_model VARCHAR(64) DEFAULT NULL,    -- 该文档向量化所用模型，换模型时据此识别需重建
    owner_id    BIGINT NOT NULL,
    tenant_tag  VARCHAR(64) NOT NULL,
    is_public   TINYINT(1) NOT NULL DEFAULT 0,
    error_msg   VARCHAR(512) DEFAULT NULL,           -- 处理失败原因
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at  TIMESTAMP NULL DEFAULT NULL,         -- 软删除时间戳，NULL=未删除（见 5.5）
    UNIQUE KEY uk_md5_owner (file_md5, owner_id),
    INDEX idx_tenant (tenant_tag),
    INDEX idx_ingest_status (ingest_status),
    INDEX idx_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 分块记录（真理来源，ES/Qdrant 是它的投影）
CREATE TABLE chunks (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    document_id   BIGINT NOT NULL,
    chunk_index   INT NOT NULL,
    content       MEDIUMTEXT NOT NULL,
    token_count   INT NOT NULL DEFAULT 0,
    -- 用于 LLM 引用回链：定位原文位置
    char_start    INT DEFAULT NULL,
    char_end      INT DEFAULT NULL,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_doc_idx (document_id, chunk_index),
    INDEX idx_document (document_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 会话与消息（多轮问答记忆）
CREATE TABLE conversations (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id     BIGINT NOT NULL,
    title       VARCHAR(255) DEFAULT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE messages (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    conversation_id BIGINT NOT NULL,
    role            ENUM('user','assistant') NOT NULL,
    content         MEDIUMTEXT NOT NULL,
    citations       JSON DEFAULT NULL,               -- 引用的 chunk 列表
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_conversation (conversation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

> **设计点**：`chunks` 表是 single source of truth，ES 和 Qdrant 都是它的「索引投影」。任何时候索引坏了，都能从 MySQL 重建 —— 这是双写架构的兜底。

### 4.2 Elasticsearch（只做 BM25）

```jsonc
// index: chunks_bm25
{
  "mappings": {
    "properties": {
      "chunk_id":    { "type": "keyword" },   // = {document_id}_{chunk_index}
      "document_id": { "type": "long" },
      "content": {
        "type": "text",
        "analyzer": "ik_max_word",            // 索引用最细粒度
        "search_analyzer": "ik_smart"         // 查询用智能粒度
      },
      // 权限过滤字段
      "owner_id":   { "type": "long" },
      "tenant_tag": { "type": "keyword" },
      "is_public":  { "type": "boolean" },
      "is_deleted": { "type": "boolean" }
    }
  }
}
```

注意：ES 这里**不再有 dense_vector 字段**，向量全部交给 Qdrant。

### 4.3 Qdrant（只做向量）

```jsonc
// collection: chunks_vec
{
  "vectors": {
    "size": 1024,                  // 取决于 embedding 模型维度
    "distance": "Cosine"
  },
  "hnsw_config": { "m": 16, "ef_construct": 100 },
  "quantization_config": {         // 标量量化省内存
    "scalar": { "type": "int8", "always_ram": true }
  }
  // point id = hash({document_id}_{chunk_index})  幂等 upsert
  // payload（用于过滤 + 回链）：
  //   document_id, chunk_index, owner_id, tenant_tag, is_public, is_deleted
}
```

> **设计点**：payload 里冗余存权限字段，使向量检索能在 Qdrant 内**先过滤再算 ANN**（pre-filtering），避免召回后再过滤导致结果不足。

### 4.4 Embedding 模型与维度约束（必须想清楚的硬约束）

向量维度和 embedding 模型**强绑定**：Qdrant collection 的 `size`（如 1024）在创建时就固定，写入的所有向量必须同维。这带来几条必须正视的约束：

1. **维度由模型决定，不可中途变更**。一旦选定模型（如某 1024 维模型），collection 维度就锁死。不同模型产出的向量即使维度相同，语义空间也不通用，**不能混在同一 collection 检索**。
2. **换模型 = 重建索引 + 全量 re-embedding**。这是一项已知的、昂贵的离线操作，不是热切换。流程：建新 collection（新维度）→ 遍历 `chunks` 表（真理来源）全量重新 embedding → 写入新 collection → 切流量 → 删旧 collection。
3. **记录每个文档用的模型**。`documents.embedding_model` 字段记录向量化时所用模型，便于：识别哪些文档还停留在旧模型、灰度迁移、排查"为什么这批文档检索效果异常"。
4. **查询向量必须与库内向量同模型**。问答时给 query 做 embedding，必须用与文档相同的模型，否则向量不可比，检索全错。

> **面试点**：被问"想换个更好的 embedding 模型怎么办"时，正确答案不是"改个配置"，而是"维度和语义空间都绑定模型，换模型是一次全量重建索引的离线迁移，我用 `embedding_model` 字段支持灰度"。能讲出这个，说明你理解向量检索的本质约束，而不只是调 API。

## 5. 摄取管线（Ingestion）

### 5.1 分片上传（断点续传 + 秒传）

设计要点：

- **秒传**：上传前先用 `file_md5` 查 `documents` 表，已存在且 `upload_status=1` 直接返回，跳过上传。
- **断点续传**：每个 5MB 分片单独上传到 MinIO `chunks/{file_md5}/{index}`，Redis 用 bitmap/set 记录已传分片。客户端中断后重新发起上传（`POST /documents`），服务端返回已传分片列表，只补传缺的。
- **合并**：所有分片到齐后用 MinIO `ComposeObject` 合并成 `merged/{file_md5}/{file_name}`，更新 `upload_status=1`，然后**发 Kafka 消息**触发摄取。

> Redis key 设计：`upload:{file_md5}:{owner_id}` → bitmap，第 i 位表示第 i 个分片是否已传。bitmap 比 set 省内存，O(1) 判断。

### 5.2 智能分块

分块直接决定检索质量。定长暴力切会切断中文句子和 Markdown 结构，导致语义破碎。本项目方案：

**策略：结构感知 + token 级滑窗 + 重叠**

1. **结构优先切分**：按文档结构边界（Markdown 标题、段落、列表项；纯文本按段落/句子）先切成语义块。
2. **token 级合并/再切**：用 tokenizer 估算 token 数，把小块合并到接近 `chunkSize`（如 512 token），过大的块按句子边界二次切分。
3. **重叠（overlap）**：相邻 chunk 保留 `overlap`（如 64 token）的重叠，避免答案正好落在切割边界被割裂。
4. **保留元数据**：记录每个 chunk 在原文的 `char_start/char_end`，用于引用回链。

> **面试点**：能讲清"为什么不能定长切"（切断语义）、"为什么要 overlap"（边界信息丢失）、"为什么按 token 而非字符"（LLM 和 embedding 都按 token 计费/限长），就能体现你真正理解 RAG 质量的源头在分块。

```go
// 分块接口（internal/indexing/chunker）
type Chunker interface {
    Chunk(text string, opts ChunkOptions) ([]Chunk, error)
}
type ChunkOptions struct {
    ChunkSize   int // 目标 token 数，默认 512
    Overlap     int // 重叠 token 数，默认 64
    Strategy    string // "markdown" | "recursive" | "sentence"
}
type Chunk struct {
    Index     int
    Content   string
    TokenCount int
    CharStart, CharEnd int
}
```

### 5.3 批量 Embedding

文档动辄几百个 chunk，逐条串行调 embedding API 会产生几百次 HTTP 往返，慢且易触发限速。本项目方案：

- **批量**：embedding API 支持一次传多条文本（如一批 16~64 条），减少 HTTP 往返。
- **并发**：多个批次用 `errgroup` 并发，受 `semaphore` 限流（如最多 4 个并发请求），避免打爆 API 限速。
- **重试**：单批失败用指数退避重试（如 3 次，base 500ms），区分可重试（5xx/超时/429）与不可重试（4xx 参数错）。
- **降级**：429 限速时退避加倍。

```go
type EmbeddingClient interface {
    // 一次嵌入一批文本，返回等长向量切片
    EmbedBatch(ctx context.Context, texts []string) ([][]float32, error)
    Dimensions() int
    Model() string
}
```

### 5.4 双写索引

worker 在一条 Kafka 消息内顺序执行，全程幂等：

```
1. 更新 documents.ingest_status = 处理中
2. 拉取 merged 文件 → Tika 抽取文本
3. 分块 → 写入 MySQL chunks 表（真理来源）
4. 批量 embedding
5. 双写：
   - Qdrant upsert（point id = hash(chunk_id)）
   - ES bulk index（doc id = chunk_id）
6. 更新 documents.ingest_status = 完成, chunk_count = N
   任一步失败 → ingest_status = 失败, error_msg = ...，消息进重试/DLQ
```

**幂等保证**：所有写操作都用 `chunk_id = {document_id}_{chunk_index}` 作 ID 做 upsert，重复消费覆盖而非重复插入。

**幽灵数据问题（关键细节）**：仅靠 upsert 覆盖还不够。设想一个文档第一次处理切成 100 个 chunk，索引了 `chunk_0..99`；后来分块策略优化、或文档内容变短，重新处理只切出 80 个 chunk。这时 upsert 只会覆盖 `chunk_0..79`，**旧的 `chunk_80..99` 不会被任何写操作触及，残留在 ES/Qdrant 里变成"幽灵数据"**，会被检索召回，造成答案引用到已不存在的内容。

解法：**重新处理前，先按 `document_id` 删除该文档在 MySQL/ES/Qdrant 的全部旧 chunk，再写新的**（delete-then-write）。删除用 `document_id` 范围删（ES `delete_by_query`、Qdrant filter delete），不依赖具体 chunk 数，因此不会漏。

> **面试点**：能主动讲出"upsert 覆盖解决不了 chunk 数变少的幽灵数据，必须 delete-then-write"，是真正踩过坑才知道的细节，区分度极高。

### 5.5 文档删除与更新的一致性

`DELETE /documents/{id}` 和重新上传都要让三个存储（MySQL/ES/Qdrant）保持一致。直接物理删三处会遇到和双写一样的难题：删了 MySQL、删 Qdrant 时失败，就会残留可被检索到的 chunk —— **这是数据越权泄漏风险**（用户以为删了，实际还能被搜出来）。

**方案：软删除 + 异步物理清理**

1. 删除请求先做一次幂等的状态更新：`documents.deleted_at = NOW()`（MySQL 是真理来源）。
2. 在线检索实际查 ES/Qdrant，因此删除请求返回成功前必须把读侧索引同步标记为不可见：ES/Qdrant chunk payload 写入 `is_deleted=true`。
3. **检索时强制下推过滤** `is_deleted = false`，并叠加多租户权限过滤 —— 一旦索引 tombstone 生效，立刻对所有查询不可见，安全边界即时生效。
4. 后台异步任务（或复用 worker）真正物理删除 ES/Qdrant 的 chunk、MySQL chunks 以及对象存储文件，失败可重试，不影响用户感知。物理清理完成后可删除 document 元数据行。

这样把"分布式多存储删除一致性"拆成"MySQL 软删除 + 索引 tombstone 立即止血 + 最终物理清理"。安全性由步骤 2/3 的读侧不可见即时保证，物理清理慢一点也无所谓。

> **面试点**：删除比写入更容易被问，因为它直接关联数据泄漏。软删除 + 检索过滤的核心价值是"安全边界即时生效，不依赖物理删除成功"。

### 5.6 状态机

```
documents.ingest_status:
  0 待处理 ──worker领取──▶ 1 处理中 ──成功──▶ 2 完成
                              └──失败/重试耗尽──▶ 3 失败(error_msg)
                              
重新上传/手动重试：3 ──▶ 0
```

### 5.7 Kafka 可靠性（重试 + DLQ + 幂等）

摄取任务"宁可慢、不可丢"，因此可靠性是重点：

- **手动提交位移**：处理成功才提交 offset，避免消息丢失（at-least-once）。
- **应用层重试**：消费到的消息处理失败，在消费者内按退避重试有限次。
- **死信队列**：重试耗尽的消息连同错误原因发到 `doc.ingest.dlq`，然后提交原 offset（避免毒丸阻塞分区）。DLQ 可人工排查后重放。
- **幂等消费**：因为是 at-least-once，同一消息可能被消费多次，靠 5.4 的幂等 upsert 保证结果正确。

> **面试点**：at-least-once + 幂等 = 实质 exactly-once 效果。能讲清"为什么不追求真正的 exactly-once"（分布式下成本极高、Kafka 事务有局限），以及 DLQ 解决的"毒丸消息阻塞消费组"问题。

## 6. 检索层（Retrieval）

检索是 RAG 质量的命脉。本项目用**两阶段架构**：召回（高召回率）→ 精排（高准确率）。

### 6.1 接口设计

```go
// internal/retrieval
type Retriever interface {
    Retrieve(ctx context.Context, req RetrieveRequest) ([]Candidate, error)
}

type RetrieveRequest struct {
    Query       string
    QueryVector []float32
    TopK        int          // 最终返回数（精排后），默认 5
    RecallK     int          // 每路召回数，默认 50
    Scope       AccessScope  // 多租户权限：owner_id + tenant_tags + public
}

type Candidate struct {
    ChunkID    string
    DocumentID int64
    Content    string
    // 各路原始信号（用于调试/可解释）
    BM25Rank   int
    VectorRank int
    FusedScore float64   // RRF 融合分
    RerankScore float64  // 精排分
}
```

**关于这些参数的取值（面试必问"为什么是这个数"）**：`RecallK=50`、`TopK=5`、RRF 的 `k=60` 都是**可配置**的，不是拍脑袋写死。其中：

- RRF 的 `k=60` 是原论文的经验值，社区与 ES 实现普遍沿用，可直接引用。
- `RecallK`（每路召回数）和 `TopK`（喂给 LLM 的最终数）则**通过第 10 节的评估集扫出来**：召回太小漏掉相关 chunk（Recall@k 上不去），太大引入噪声且拖慢 rerank；TopK 太小信息不足，太大撑爆 LLM context 窗口、增加幻觉。
- 正确的叙事是闭环的：「我搭了评估集 → 用 Recall@k / MRR 扫 RecallK 和 TopK → 选出召回率与延迟的平衡点」。这把"检索参数"和"评估体系"两节串成一个故事，而不是孤立的魔法数字。

> 详见 [第 10 节 RAG 评估体系](#10-rag-评估体系)：参数调优本质是一次小型离线实验。

### 6.2 第一阶段：并行召回

两路检索**并行**执行（`errgroup`），各取 `RecallK` 条，各带权限过滤：

- **BM25 路**（ES）：`match` query on `content` + `bool.filter` 权限过滤。可加 `match_phrase` 短语 boost，并对中文 query 做归一化（去停用词、标点，提取关键词）以提升关键词匹配质量。
- **向量路**（Qdrant）：query vector KNN + payload filter 权限过滤（pre-filtering）。

权限过滤逻辑（两路一致）：`is_deleted == false AND (owner_id == 当前用户 OR tenant_tag ∈ 用户租户集 OR is_public == true)`。

### 6.3 第二阶段：RRF 融合

把两路排序列表融合（算法见 3.2）：

```go
// internal/retrieval/fusion
func ReciprocalRankFusion(lists [][]Candidate, k int) []Candidate {
    // k 默认 60
    scores := map[string]float64{}
    keep   := map[string]Candidate{}
    for _, list := range lists {
        for rank, c := range list { // rank 从 0 开始，公式里 +1
            scores[c.ChunkID] += 1.0 / float64(k + rank + 1)
            keep[c.ChunkID] = c
        }
    }
    // 按 scores 降序排序 → 输出
}
```

### 6.4 第三阶段：Rerank 精排

对 RRF 后的候选（如 top-30）调用 rerank 模型（cross-encoder，query 与每个候选拼接打分），重排取最终 `TopK`：

```go
type Reranker interface {
    Rerank(ctx context.Context, query string, candidates []Candidate, topK int) ([]Candidate, error)
}
```

- 实现一：rerank API（如 OpenAI 兼容 / 专用 rerank 服务）。
- 实现二（降级）：无 rerank 服务时退回 RRF 排序，接口不变。

**超时降级（关键）**：rerank 在问答主链路上，且通常是第三方/独立服务，延迟不可控。如果不设防，rerank 卡 5 秒，用户的问答首字延迟就被拖 5 秒。设计上：

- 给 rerank 单独设一个**紧超时**（如 800ms，独立于整体请求超时），用 `context.WithTimeout` 控制。
- 超时或报错时，**不阻塞、不报错**，直接降级用 RRF 的 top-k 结果继续生成。rerank 是"锦上添花"，不是"不可或缺"——召回+RRF 已经能给出可用结果，精排只是优化排序。
- 降级事件打点到 metrics（`rerank_fallback_total`），便于观测 rerank 服务健康度。

> **面试点**：能说出"我把 rerank 设成可降级的旁路、设独立超时、降级有埋点"，体现的是"在线链路上对外部依赖要有熔断意识"——这正是区分 demo 和生产系统的地方。

### 6.5 查询增强（进阶亮点）

- **查询改写**：多轮对话时，把"它的参数是多少"结合上下文改写成自包含查询（"X 模型的参数是多少"），再检索。用 LLM 做。
- **HyDE（Hypothetical Document Embeddings）**：先让 LLM 生成一个"假设答案"，用假设答案的向量去检索，往往比用问题向量召回更准（因为答案和文档在同一语义空间）。作为可选开关。

> **面试点**：HyDE 的直觉 ——「问题」和「文档」是不同文体，直接拿问题向量匹配文档向量有 gap；先生成一段假想答案，它的文体更接近真实文档，召回更准。

## 7. 生成层（Generation）

生成是 RAG 区别于"语义搜索"的关键一环：检索只返回相关片段，生成才把片段综合成有出处的答案。

### 7.1 流程

```
1. 检索得到 top-k chunk（带 document_id / chunk_index / content）
2. 拼装 Context：每个 chunk 编号 [1][2][3]...，附带来源文件名
3. 构造 Prompt：System(角色+grounding约束) + Context + History + Question
4. 调 LLM 流式生成
5. SSE 逐 token 推给前端
6. 生成结束后解析答案里的 [n] 引用标记，回填 citations 元数据
7. 持久化 message（含 citations）到 MySQL
```

### 7.2 Prompt 设计（防幻觉 grounding）

System prompt 的核心约束：

```
你是企业知识库助手。严格依据【参考资料】回答问题。
规则：
1. 只使用参考资料中的信息，不要编造。
2. 每个论断后用 [编号] 标注来源，如 [1][2]。
3. 如果参考资料不足以回答，明确说"根据现有资料无法回答"，不要猜测。
4. 用与问题相同的语言回答。
```

> **面试点**：grounding 是 RAG 抑制幻觉的关键。"无法回答就说无法回答"这条约束，比任何后处理都有效。引用标注让答案**可溯源、可验证**，这是企业场景的硬需求。

### 7.3 流式 SSE

```go
// internal/generation
type Generator interface {
    // 流式生成，通过 channel 推送增量 token
    GenerateStream(ctx context.Context, req GenerateRequest) (<-chan StreamChunk, error)
}
type StreamChunk struct {
    Delta     string      // 增量文本
    Citations []Citation  // 结束时一次性返回引用
    Done      bool
    Err       error
}
```

HTTP 层用 Gin 的 `c.Stream()` 把 channel 转成 SSE 事件流（`text/event-stream`）。客户端中断时通过 `ctx` 取消，及时停止 LLM 调用省 token。

### 7.4 引用回链

答案里的 `[1]` 对应 Context 第 1 个 chunk，回填为：

```json
{ "index": 1, "document_id": 42, "file_name": "架构规范.pdf",
  "chunk_index": 7, "char_start": 1200, "char_end": 1720 }
```

前端可据此跳转到原文位置高亮 —— 完整的"答案 → 出处"闭环。

### 7.5 会话记忆与查询改写

多轮对话历史存 Redis（`conv:{conversation_id}` → 最近 N 轮），同时落库 MySQL `messages`。下一轮带上历史拼进 prompt。Redis 控制窗口大小避免 prompt 超长。

**查询改写的延迟权衡（关键决策）**：多轮场景下，用户会说"它的参数呢"这种带指代/省略的问题，直接拿去检索召回会很差，需要结合上下文改写成自包含的查询（"X 模型的参数是多少"）。但改写要**额外调一次 LLM**，于是单轮问答的链路变成：

```
改写(LLM调用①) → 检索 → 生成(LLM调用②)
```

两次串行 LLM 调用，首字延迟（TTFT）显著增加。代价不能无脑付，因此**条件触发**：

- 第一轮对话（无历史）→ 不改写，问题本身就是自包含的。
- 后续轮次：先用**廉价的启发式判断**是否需要改写（是否含指代词"它/这个/那个"、是否过短、是否缺主语）。只有疑似依赖上下文时才触发改写 LLM 调用。
- 或用一次**小模型/快模型**做改写，把改写的延迟成本压到最低。

> **面试点**：被问"多轮怎么处理指代"时，初级答案是"把历史拼进去 / 每轮都改写"，高级答案是"改写有 LLM 往返成本，我用启发式条件触发，只在检测到指代或省略时才改写，平衡召回质量与首字延迟"。体现的是对**在线延迟预算**的敏感。

## 8. 多租户与权限

### 8.1 模型

每个 chunk 都带三个权限维度（冗余存进 ES、Qdrant payload 和 MySQL）：

- `owner_id` — 上传者，私有文档只有本人可见。
- `tenant_tag` — 所属租户/组织，同租户成员可见。
- `is_public` — 全局公开。

### 8.2 检索时的访问范围

请求进来时，由 JWT 解析出 `user_id`，再查 `user_tenants` 得到用户所属租户集，构造 `AccessScope`：

```
可见 ⟺ chunk.owner_id == user_id
     OR chunk.tenant_tag ∈ user.tenant_tags
     OR chunk.is_public == true
```

这个条件同时下推到 ES 的 `bool.filter` 和 Qdrant 的 payload filter，**在检索引擎内过滤**，而不是召回后在应用层过滤（后者会导致 top-k 不足、性能差、信息泄漏风险）。

> **面试点**：权限过滤"下推到存储层"是多租户系统的标准做法。在应用层过滤的坑：召回 50 条过滤剩 3 条，结果不够；且分页失效。

### 8.3 安全

**认证与密钥**

- 所有 `/api/v1` 业务接口走 JWT 中间件。
- access token（短期，如 2h）+ refresh token（长期，如 7d，存 Redis 可吊销）。
- 密码 bcrypt。
- **本地密钥写入私有配置文件，绝不提交 git**。`configs/config.yaml` 被 `.gitignore` 忽略，开发环境可以直接填写 API key、DB 密码、JWT 密钥；仓库只保留 `configs/config.example.yaml` 模板。

**越权访问防护（企业知识库的核心风险）**

知识库最大的安全风险不是被攻破，而是**用户 A 看到了用户 B 的文档**。除了 8.2 的检索权限过滤，还有两个隐蔽的越权点必须堵：

- **语义缓存必须按租户隔离 key**。9.3 提到的语义缓存（相似问题直接返回历史答案）有个致命陷阱：如果缓存 key 只用问题文本的哈希，那么用户 B 问了和用户 A 相似的问题，会直接命中 A 的缓存答案，**而 A 的答案是基于 A 的私有文档生成的** —— 这就是一次跨租户数据泄漏。缓存 key 必须包含 `AccessScope`（如 `hash(query) + owner_id + sorted(tenant_tags)`），让不同权限范围的用户各自独立缓存。
- **引用回链要二次校验权限**。答案里的 `[1]` 引用回链到某 chunk，前端请求原文时，服务端要重新校验当前用户对该 chunk 所属文档是否有访问权，不能凭引用 ID 直接返回。

**文件上传安全**

- **类型校验**：不能只看扩展名，要校验文件头 magic number，防止伪装的可执行文件。只放行白名单内的文档类型（PDF/Office/Markdown/txt）。
- **大小限制**：单文件设上限（如 100MB），防止超大文件打爆存储与处理管线。
- **存储隔离**：MinIO 对象路径带 `owner_id`/`tenant`，避免越权拉取原始文件。

> **面试点**：语义缓存的跨租户泄漏是个非常隐蔽、又非常能体现安全意识的点。能主动说"我上了语义缓存，但缓存 key 必须带权限范围，否则会跨租户泄漏"，面试官会认为你对安全有真实的敏感度，而不是事后补丁。

---

## 9. 可观测性与可靠性

### 9.1 三大支柱

- **结构化日志**（zap）：每条日志带 `trace_id`、`user_id`、关键业务字段。
- **链路追踪**（OpenTelemetry）：一次问答请求横跨 embedding → ES → Qdrant → rerank → LLM，trace 把整条链路串起来，能一眼看出哪一段慢。导出到 Jaeger。
- **指标**（Prometheus）：QPS、各阶段延迟（P50/P95/P99）、Kafka 消费积压、DLQ 计数、LLM token 用量、缓存命中率。Grafana 看板。

> **面试点**：RAG 链路长，没有 trace 就是黑盒。能说出"我用 trace 定位到 rerank 是延迟瓶颈，于是加了缓存"这种话，远胜空谈。

### 9.2 可靠性清单

- 优雅关闭：监听 SIGTERM，停止接收新请求/消息，等在途处理完成，关闭连接池。
- 依赖检查：API 启动时初始化并校验 MySQL、Redis、ES、Qdrant、MinIO 等依赖；Docker Compose 负责容器级 healthcheck。
- 限流：基于 Redis 的租户级令牌桶，防止单租户打爆 LLM 配额。
- 超时与熔断：对 LLM/embedding/rerank 等外部调用设超时和上下文取消。
- 连接池：DB（gorm + dbresolver 读写分离）、HTTP client 复用。

### 9.3 缓存

- **embedding 缓存**：相同文本的向量缓存（Redis，key=hash(text)+model），避免重复嵌入。
- **语义缓存（进阶）**：相似问题（向量相似度超阈值）直接返回历史答案，省一次完整 RAG。**缓存 key 必须带权限范围**，否则跨租户泄漏，详见 [8.3](#83-安全)。

### 9.4 失败场景与降级策略

大厂面试官特别爱问 failure mode。把链路上每个外部依赖的失败方式和应对列清楚，体现的是"考虑过边界"的工程成熟度。

| 失败场景 | 影响 | 应对策略 |
|---|---|---|
| **Tika 解析失败**（加密 PDF、扫描件无文本层、损坏文件） | 该文档无法摄取 | 标 `ingest_status=失败` + `error_msg`，不重试（重试也没用）；前端展示失败原因引导用户。OCR 列为 non-goal |
| **文本抽取为空**（纯图片 PPT） | 无 chunk 可索引 | 同上，明确提示"未提取到文本内容" |
| **单 chunk 超 embedding 输入上限**（中文 token 膨胀） | 该批 embedding 报错 | 分块阶段就按 token 上限二次截断（见 5.2），写入前再做一次长度兜底 |
| **embedding API 限速 / 欠费 / 超时** | 摄取中断 | 可重试错误（429/5xx/超时）指数退避重试；重试耗尽 → 整条 Kafka 消息进 DLQ，文档标失败可手动 reingest |
| **Qdrant / ES 写入失败**（双写一边挂） | 索引不一致 | 整条消息失败重试（幂等 upsert 保证安全）；最终进 DLQ。MySQL chunks 是真理来源，随时可重建 |
| **Kafka 消息重复消费** | 可能重复索引 | 幂等 upsert（chunk_id 作 ID）保证结果正确，见 5.7 |
| **Rerank 服务超时 / 不可用** | 在线问答受阻 | 独立紧超时（800ms）+ 降级用 RRF 结果，不阻塞生成，见 6.4 |
| **检索召回为空**（无相关文档） | 无 context | 不硬塞，直接让 LLM 回答"根据现有资料无法回答"（grounding prompt 已约束），不编造 |
| **LLM API 超时 / 限速** | 无法生成答案 | 设超时；返回明确错误给前端（可带"稍后重试"）；可选 fallback 到备用模型 |
| **LLM 流式生成中途断连**（客户端关闭/网络中断） | 答案不完整 | 通过 `ctx` 取消及时停止 LLM 调用省 token；已生成部分不落库（避免存半句话），或标记 `incomplete` |
| **单租户高频请求打爆 LLM 配额** | 影响其他租户 | 租户级令牌桶限流（9.2），超限返回 429 |

> **面试点**：这张表本身就是答案库。被问任何"X 挂了怎么办"，直接命中。核心思想分两类：**摄取链路**失败可异步重试/进 DLQ（用户不在线等），**在线问答链路**失败要快速降级或明确报错（用户在等，不能卡）。

---

## 10. RAG 评估体系

> 大多数 RAG 项目止步于"能跑"，但说不清"效果有多好"。一套可量化的评估体系，能把"我做了个 RAG"变成"我能用数据说明每个设计的收益"。

### 10.1 评测集

构造 `(问题, 标准答案, 相关 chunk id 集)` 的评测集（可半自动：用 LLM 从文档生成 QA 对，人工抽检）。

### 10.2 检索指标（离线，`cmd/eval`）

- **Recall@k**：top-k 里命中相关 chunk 的比例 —— 衡量召回够不够。
- **MRR（Mean Reciprocal Rank）**：第一个相关结果排名的倒数均值 —— 衡量排得准不准。
- **nDCG@k**：带位置权重的相关性 —— 综合指标。

用这套指标横向对比：纯向量 vs 纯 BM25 vs RRF 融合 vs +Rerank，**用数据证明每一层的增益**。同时，这套指标也是 [6.1 检索参数](#61-接口设计)（`RecallK`/`TopK`）的调优依据——扫不同取值看 Recall@k 与延迟的平衡点，参数由实验定，不靠拍脑袋。

### 10.3 生成指标（LLM-as-judge）

- **忠实度（Faithfulness）**：答案是否都能从 context 推出（不幻觉）。用 LLM 裁判打分。
- **答案相关性（Answer Relevancy）**：答案是否切题。
- **上下文精确率/召回率**：检索到的 context 有多少真正被用上。

> **面试点**：能讲"我做了消融实验，加上 Rerank 后 MRR 从 0.62 提升到 0.78"，就把项目从"我做了个 RAG"提升到"我做了个**可度量、可优化**的 RAG"。具体数字等实现后用真实评测集跑出来填——面试时务必用真数据，编不得。

---

## 11. API 设计

RESTful + SSE，统一响应包装 `{ code, message, data }`。JSON 字段统一 camelCase，路径段小写短横线，资源用复数名词，动作交给 HTTP 方法。

### 11.1 命名规范

| 规则 | 做法 |
|---|---|
| 资源用复数名词 | `/documents`、`/conversations`，而非 `/upload`、`/chat` |
| 动词交给 HTTP 方法 | GET 查 / POST 建 / PUT 替换 / PATCH 改 / DELETE 删 |
| 路径段小写 + 短横线 | `/auth/refresh`，不用驼峰 `/auth/refreshToken` |
| 层级表达从属 | 消息属于会话 → `/conversations/{id}/messages` |
| JSON 字段 camelCase | 与 Go json tag、前端 JS 一致 |
| 过滤/分页用 query 参数 | `?page=1&size=20&status=done` |

### 11.2 端点清单

```
# 认证（无需鉴权）
POST   /api/v1/auth/register                            注册
POST   /api/v1/auth/login                               登录 → accessToken + refreshToken
POST   /api/v1/auth/refresh                             刷新 accessToken
POST   /api/v1/auth/logout                              登出（吊销 refreshToken）

# 用户与租户（需鉴权）
GET    /api/v1/users/me                                 当前用户信息
PATCH  /api/v1/users/me                                 改资料（含默认租户 primaryTenant）
GET    /api/v1/tenants                                  我的租户列表

# 文档 —— S3 分片上传风格（需鉴权）
POST   /api/v1/documents                                发起上传（返回秒传命中 / 已传分片列表）
PUT    /api/v1/documents/{id}/parts/{partNumber}        上传单个分片（PUT 幂等，失败可重传）
POST   /api/v1/documents/{id}/complete                  完成上传 → 合并 + 触发摄取
GET    /api/v1/documents                                我可见的文档列表（分页 + 过滤）
GET    /api/v1/documents/{id}                            文档详情（含上传/摄取状态）
DELETE /api/v1/documents/{id}                            删除文档（级联删 MySQL/ES/Qdrant）
POST   /api/v1/documents/{id}/reingest                  重试失败的摄取（可选）

# 检索与问答（需鉴权）
POST   /api/v1/search                                   混合检索，返回 chunk 列表（搜索模式/调试）
POST   /api/v1/conversations                            新建会话
GET    /api/v1/conversations                            会话列表
GET    /api/v1/conversations/{id}                        会话历史（含消息）
DELETE /api/v1/conversations/{id}                        删除会话
POST   /api/v1/conversations/{id}/messages              提问，SSE 流式返回答案 + 引用 ← RAG 核心

# 运维（无版本前缀、无鉴权）
GET    /metrics
```

### 11.3 设计说明

- **上传走 S3 Multipart Upload 风格**：`POST /documents` 发起（一个接口同时完成秒传判断与已传分片查询），`PUT .../parts/{n}` 传分片，`POST .../complete` 完成。分片上传用 **PUT 而非 POST**，因为 PUT 幂等，同一分片重传不会出问题，天然适配失败重试。
- **提问是"在会话里新建消息"**：用 `POST /conversations/{id}/messages` 而非独立的 `/chat`，这才是 RESTful 表达，也天然解决了答案归属哪个会话。新会话可先 `POST /conversations` 建，或在请求体里传 `conversationId: null` 自动建。
- **`/search` 用 POST 而非 GET**：query 可能很长、还带 `topK`/`useHyde` 等选项，塞 query string 不优雅，用 body 更合适（ES、GitHub 的 search 也是 POST）。
- **刻意不做的接口**：支持文件类型是固定业务规则（前端硬编码或错误提示即可，不占接口）；上传状态查询并入 `GET /documents/{id}`；秒传/续传判断并入 `POST /documents` —— 避免接口碎片化。

### 11.4 问答接口（SSE）请求/响应

```jsonc
// 请求
POST /api/v1/conversations/{id}/messages
{
  "query": "X 模型的最大上下文是多少？",
  "options": { "useHyde": false, "topK": 5 }
}

// 响应：SSE 流（Content-Type: text/event-stream）
// event: delta  → {"delta":"根据"}
// event: delta  → {"delta":"资料，"}
// ...
// event: done   → {"citations":[{"index":1,"documentId":42,"fileName":"架构规范.pdf","chunkIndex":7}]}
```

---

## 12. 目录结构

```
ragkb/
├── cmd/
│   ├── api/                     # HTTP API 进程入口，只做启动与优雅关闭
│   └── worker/                  # Kafka worker 进程入口，只做启动与优雅关闭
├── configs/
│   ├── config.example.yaml      # 可提交的配置模板
│   └── config.yaml              # 本地私有配置，忽略 git
├── deploy/
│   └── docker-compose.yaml      # 本地依赖：MySQL/Redis/ES/Qdrant/MinIO/Kafka/Tika
├── migrations/
│   └── init.sql                 # MySQL 初始化建表脚本
└── internal/
    ├── bootstrap/               # 进程级依赖装配：api / worker
    ├── config/                  # yaml 配置结构、默认值、校验
    ├── domain/                  # 领域模型、领域错误、仓储接口
    │   ├── document/
    │   └── user/
    ├── service/                 # API 用例编排：认证、用户、文档上传
    ├── handler/                 # HTTP handler 与 DTO
    │   ├── auth/
    │   ├── document/
    │   ├── shared/
    │   └── user/
    ├── indexing/                # worker 索引维护用例：抽取、分块、embedding、双写索引、删除清理
    │   └── chunker/             # 分块策略实现
    ├── retrieval/               # 在线检索用例：并行召回、RRF 融合、rerank 降级、chunk 回填
    ├── infra/                   # 外部基础设施适配，具体 SDK 不向业务层泄漏
    │   ├── elasticsearch/
    │   ├── embedding/
    │   ├── jwt/
    │   ├── kafka/
    │   ├── minio/
    │   ├── mysql/
    │   ├── qdrant/
    │   ├── redis/
    │   └── tika/
    ├── observability/           # 日志、后续 trace/metrics
    ├── response/                # 统一 HTTP 响应包装
    └── server/                  # Gin router 与中间件
```

目录边界原则：

- `cmd/*` 不写业务逻辑，只负责读取配置、创建 logger、调用 `bootstrap`、监听退出信号。
- `domain/*` 只放领域模型、领域错误、仓储接口；不依赖 MySQL/Redis/ES/Qdrant 等具体实现。
- `service/*` 面向 HTTP API 用例，处理上传、用户、认证等同步业务流程。
- `indexing/*` 面向 worker 索引维护用例，编排 Tika、分块、embedding、MySQL chunks、ES、Qdrant，并处理删除后的异步物理清理。
- `retrieval/*` 面向在线检索用例，编排 BM25/向量召回、RRF、rerank 降级与 chunk 内容回填；只依赖接口，不依赖 ES/Qdrant SDK。
- `infra/*` 放外部系统适配器；第三方 SDK 类型尽量不穿透到 `domain/service/indexing/retrieval`。
- `handler/shared` 放 handler 层共享的解析/错误映射工具，不放业务规则。

---

## 13. 分阶段实施路线

| 阶段 | 内容 | 产出 | 可写的简历点 |
|---|---|---|---|
| **0 脚手架** | 目录布局、config、docker-compose 全套依赖、优雅关闭、zap 日志、健康检查、golang-migrate 迁移、Makefile | 空骨架能跑起来、依赖能连通 | 工程化基建 |
| **1 摄取管线** | 分片上传/断点续传/秒传、智能分块、批量 embedding、ES+Qdrant 幂等双写、Kafka 重试+DLQ | 上传文档 → 自动索引完成 | 异步管线、双写一致性、消息可靠性 |
| **2 混合检索** | 并行 BM25+向量召回、权限过滤下推、RRF 融合、Rerank 精排、`/search` 接口 | 检索接口返回融合精排结果 | 混合检索、RRF、两阶段精排 |
| **3 生成闭环** | Context 拼装+引用、LLM 流式 SSE、grounding prompt、会话记忆、`POST /conversations/{id}/messages` 接口 | 完整 RAG 问答能跑 | RAG 闭环、流式、引用溯源 |
| **4 进阶亮点** | 查询改写/HyDE、RAG 评估体系(eval CLI)、OTel trace + Prometheus 指标、租户限流、缓存 | 指标看板 + 评测报告 | 可观测性、RAG 评估、性能优化 |
| **5 收尾** | 单测 + testcontainers 集成测试、CI、README 架构图、k6 压测 | 测试通过 + 量化数据 | 测试工程、CI、性能数据 |

做完 0–3 = 能面大厂的完整 RAG；4–5 = 拉开差距、体现深度。

---

## 14. 简历技术点清单与面试问答准备

### 14.1 简历可写条目（按 STAR 提炼）

- 设计并实现**多租户文档 RAG 平台**，支持 PDF/Office 等文档的上传、解析、检索与问答全链路。
- 基于 **Kafka 构建异步摄取管线**，实现分片上传/断点续传/秒传，引入**重试 + 死信队列 + 幂等消费**保证摄取可靠性。
- 实现 **ES(BM25) + Qdrant(向量) 双路混合检索**，用 **RRF 算法融合**多路结果，叠加 **Rerank 精排**，相比单路检索 MRR 提升 X%（用真实评测数据）。
- 补全 **LLM 流式生成（SSE）**，通过 **grounding prompt + 引用标注**抑制幻觉、实现答案可溯源。
- 搭建 **RAG 评估体系**（Recall@k / MRR / 忠实度），用消融实验量化各检索层增益。
- 基于 **OpenTelemetry + Prometheus** 构建全链路可观测性，定位并优化检索瓶颈。

### 14.2 高频面试问答（必背）

1. **为什么不只用 ES 的 dense_vector？** → 见 3.1（职责单一、量化省内存、可独立扩容；代价是双写一致性，用单写入方+幂等 upsert 解决）。
2. **双写一致性怎么保证？** → 见 3.1（单写入方 + 幂等 upsert + 失败整体重试 → 最终一致；MySQL chunks 表是真理来源可重建）。
3. **RRF 为什么不直接加分数？** → 见 3.2（量纲不一致，RRF 用排名）。
4. **召回了为什么还要 Rerank？** → 见 3.3（双塔召回快但糙，交叉编码精排准但慢，两阶段架构）。
5. **怎么防幻觉？** → 见 7.2（grounding 约束 + 引用标注 + "无法回答就说无法回答"）。
6. **怎么证明 RAG 效果好？** → 见第 10 节（评测集 + Recall@k/MRR + LLM 裁判忠实度 + 消融实验）。
7. **Kafka 消息丢了/重复了怎么办？** → 见 5.7（手动提交 offset 保证不丢 = at-least-once，幂等消费保证不重，DLQ 处理毒丸）。
8. **分块为什么不能定长切？** → 见 5.2（切断语义，要结构感知 + overlap + token 级）。
9. **多租户权限怎么做？** → 见第 8 节（三维权限下推到 ES/Qdrant 过滤，不在应用层过滤）。
10. **链路这么长怎么排查慢？** → 见 9.1（OTel trace 串起 embedding→检索→rerank→LLM）。
11. **重新处理文档，chunk 变少了会怎样？** → 见 5.4（upsert 覆盖不掉多出来的旧 chunk，会成幽灵数据被召回，必须 delete-then-write）。
12. **删除文档怎么保证不被搜出来？** → 见 5.5（软删除 `deleted_at` + 检索强制过滤，安全边界即时生效，物理清理异步做）。
13. **想换更好的 embedding 模型怎么办？** → 见 4.4（维度和语义空间绑定模型，换模型 = 全量重建索引的离线迁移，用 `embedding_model` 字段灰度）。
14. **Rerank 服务挂了 / 很慢怎么办？** → 见 6.4（独立紧超时 + 降级回 RRF 结果，不阻塞生成，降级有埋点）。
15. **多轮对话的指代怎么处理？** → 见 7.5（查询改写有 LLM 往返成本，用启发式条件触发，平衡召回与首字延迟）。
16. **上了语义缓存，安全上要注意什么？** → 见 8.3（缓存 key 必须带权限范围，否则跨租户命中别人的私有答案，是数据泄漏）。
17. **为什么不用 LangChain？** → 见 3.7（框架适合原型；自研为性能/可观测可控、吃透原理，且不贬低框架）。
18. **各种依赖挂了怎么办？** → 见 9.4（失败场景表：摄取链路可异步重试/DLQ，在线链路要快速降级或明确报错）。

---

> 文档版本：v1（设计阶段）。实现过程中如有调整会同步更新本文件。下一步：经确认后从阶段 0 脚手架开始编码。
