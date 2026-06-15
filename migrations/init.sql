-- ============================================================
-- RAGKB 数据库建表脚本
-- 库：业务元数据强一致存储（向量/全文索引在 Qdrant/ES，不在这里）
-- 字符集统一 utf8mb4，引擎 InnoDB
-- ============================================================

-- ------------------------------------------------------------
-- 用户表
-- ------------------------------------------------------------
CREATE TABLE users (
    id          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '用户主键ID，自增',
    username    VARCHAR(64)  NOT NULL                COMMENT '用户名，全局唯一，登录账号',
    password    VARCHAR(255) NOT NULL                COMMENT '密码，bcrypt 加密后的哈希值，绝不存明文',
    role        ENUM('USER','ADMIN') NOT NULL DEFAULT 'USER' COMMENT '用户角色：USER=普通用户，ADMIN=管理员',
    created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间，每次修改自动刷新',
    PRIMARY KEY (id),
    UNIQUE KEY uk_username (username) COMMENT '用户名唯一索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户表';


-- ------------------------------------------------------------
-- 租户/组织表（多租户隔离的基本单元，支持组织树）
-- ------------------------------------------------------------
CREATE TABLE tenants (
    id          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '租户主键ID，自增',
    tag         VARCHAR(64)  NOT NULL                COMMENT '租户标识符，全局唯一，如 PRIVATE_alice（个人空间）或 TEAM_infra（团队）',
    name        VARCHAR(128) NOT NULL                COMMENT '租户显示名称',
    parent_tag  VARCHAR(64)  DEFAULT NULL            COMMENT '父租户标识，为空表示顶层；用于构建组织层级树',
    created_by  BIGINT       NOT NULL                COMMENT '创建者用户ID，关联 users.id',
    created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_tag (tag)              COMMENT '租户标识唯一索引',
    KEY idx_parent (parent_tag)          COMMENT '父租户索引，便于查子组织',
    KEY idx_created_by (created_by)      COMMENT '创建者索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='租户/组织表';


-- ------------------------------------------------------------
-- 用户-租户关联表（多对多：一个用户可属于多个租户）
-- ------------------------------------------------------------
CREATE TABLE user_tenants (
    user_id     BIGINT      NOT NULL COMMENT '用户ID，关联 users.id',
    tenant_tag  VARCHAR(64) NOT NULL COMMENT '租户标识，关联 tenants.tag',
    is_primary  TINYINT(1)  NOT NULL DEFAULT 0 COMMENT '是否为该用户的默认租户：1=是（上传文件默认归属），0=否',
    created_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '加入时间',
    PRIMARY KEY (user_id, tenant_tag) COMMENT '联合主键，防止重复关联',
    KEY idx_tenant (tenant_tag)       COMMENT '租户索引，便于查某租户下所有成员'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户与租户的多对多关联表';


-- ------------------------------------------------------------
-- 文档表（一个逻辑文件，按 MD5 去重实现秒传）
-- ------------------------------------------------------------
CREATE TABLE documents (
    id            BIGINT       NOT NULL AUTO_INCREMENT COMMENT '文档主键ID，自增',
    file_md5      VARCHAR(32)  NOT NULL                COMMENT '文件内容MD5，用于秒传与去重',
    file_name     VARCHAR(255) NOT NULL                COMMENT '原始文件名（含扩展名）',
    file_ext      VARCHAR(16)  NOT NULL                COMMENT '文件扩展名，如 pdf/docx/md',
    total_size    BIGINT       NOT NULL                COMMENT '文件总字节数',
    upload_status TINYINT      NOT NULL DEFAULT 0      COMMENT '上传状态：0=分片上传中，1=已合并完成',
    ingest_status TINYINT      NOT NULL DEFAULT 0      COMMENT '摄取状态：0=待处理，1=处理中，2=完成，3=失败',
    chunk_count   INT          NOT NULL DEFAULT 0      COMMENT '该文档被切分成的分块数量，处理完成后回填',
    embedding_model VARCHAR(64) DEFAULT NULL            COMMENT '该文档向量化所用的 embedding 模型，换模型时据此识别哪些文档需重建索引',
    owner_id      BIGINT       NOT NULL                COMMENT '上传者用户ID，关联 users.id（私有权限维度）',
    tenant_tag    VARCHAR(64)  NOT NULL                COMMENT '所属租户标识（租户权限维度）',
    is_public     TINYINT(1)   NOT NULL DEFAULT 0      COMMENT '是否全局公开：1=所有人可见，0=受权限控制',
    error_msg     VARCHAR(512) DEFAULT NULL            COMMENT '摄取失败时的错误原因，成功为空',
    created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    deleted_at    TIMESTAMP    NULL DEFAULT NULL       COMMENT '软删除时间戳，NULL=未删除；删除时置为当前时间，检索时过滤掉非 NULL 的记录',
    PRIMARY KEY (id),
    UNIQUE KEY uk_md5_owner (file_md5, owner_id) COMMENT '同一用户同一文件唯一，支撑秒传判断',
    KEY idx_tenant (tenant_tag)                  COMMENT '租户索引，便于按租户列文档',
    KEY idx_ingest_status (ingest_status)        COMMENT '摄取状态索引，便于 worker 捞待处理任务',
    KEY idx_deleted_at (deleted_at)              COMMENT '软删除索引，便于过滤已删除文档与异步清理'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='文档表（逻辑文件元数据）';


-- ------------------------------------------------------------
-- 分块表（文档切分后的文本块，是 ES/Qdrant 索引的"真理来源"，可据此重建索引）
-- ------------------------------------------------------------
CREATE TABLE chunks (
    id           BIGINT     NOT NULL AUTO_INCREMENT COMMENT '分块主键ID，自增',
    document_id  BIGINT     NOT NULL                COMMENT '所属文档ID，关联 documents.id',
    chunk_index  INT        NOT NULL                COMMENT '分块在文档内的序号，从0开始',
    content      MEDIUMTEXT NOT NULL                COMMENT '分块的文本内容',
    token_count  INT        NOT NULL DEFAULT 0      COMMENT '该分块的 token 数量，用于控制 embedding/LLM 长度',
    char_start   INT        DEFAULT NULL            COMMENT '在原文中的起始字符位置，用于引用回链定位',
    char_end     INT        DEFAULT NULL            COMMENT '在原文中的结束字符位置，用于引用回链定位',
    created_at   TIMESTAMP  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_doc_idx (document_id, chunk_index) COMMENT '文档内分块序号唯一，支撑幂等重建',
    KEY idx_document (document_id)                    COMMENT '文档索引，便于按文档批量查/删分块'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='文档分块表（索引数据的真理来源）';


-- ------------------------------------------------------------
-- 会话表（一次多轮问答的容器）
-- ------------------------------------------------------------
CREATE TABLE conversations (
    id          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '会话主键ID，自增',
    user_id     BIGINT       NOT NULL                COMMENT '所属用户ID，关联 users.id',
    title       VARCHAR(255) DEFAULT NULL            COMMENT '会话标题，可由首条提问自动生成',
    created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    KEY idx_user (user_id) COMMENT '用户索引，便于列出某用户的会话'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='问答会话表';


-- ------------------------------------------------------------
-- 消息表（会话中的每一条提问/回答）
-- ------------------------------------------------------------
CREATE TABLE messages (
    id              BIGINT     NOT NULL AUTO_INCREMENT COMMENT '消息主键ID，自增',
    conversation_id BIGINT     NOT NULL                COMMENT '所属会话ID，关联 conversations.id',
    role            ENUM('user','assistant') NOT NULL  COMMENT '消息角色：user=用户提问，assistant=AI回答',
    content         MEDIUMTEXT NOT NULL                COMMENT '消息文本内容',
    citations       JSON       DEFAULT NULL            COMMENT '引用来源列表（仅 assistant 消息有），JSON存储引用的分块及原文位置',
    created_at      TIMESTAMP  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (id),
    KEY idx_conversation (conversation_id) COMMENT '会话索引，便于按会话拉取历史消息'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='会话消息表';
