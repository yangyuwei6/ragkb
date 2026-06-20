package document

import "context"

// DocumentListQuery 表示文档列表查询条件。
type DocumentListQuery struct {
	OwnerID int64
	Page    int
	Size    int
}

// DocumentRepo 定义文档领域的持久化能力。
type DocumentRepo interface {
	// Create 创建一条文档元数据记录。
	Create(ctx context.Context, d *Document) error

	// GetByID 查询未删除的文档。
	GetByID(ctx context.Context, id int64) (*Document, error)

	// GetByIDAny 查询文档，不过滤软删除记录。
	// 删除、恢复、审计这类场景需要看到完整状态。
	GetByIDAny(ctx context.Context, id int64) (*Document, error)

	// GetByMD5AndOwner 按文件 MD5 和上传者查询未删除文档，用于秒传和断点续传判断。
	GetByMD5AndOwner(ctx context.Context, md5 string, ownerID int64) (*Document, error)

	// ListByOwner 分页列出某个用户拥有的未删除文档。
	ListByOwner(ctx context.Context, q DocumentListQuery) ([]Document, int64, error)

	// MarkMerged 标记分片已合并完成，并记录最终文件大小。
	MarkMerged(ctx context.Context, id int64, totalSize int64) error

	// MarkIngestProcessing 标记文档进入摄取处理中状态。
	MarkIngestProcessing(ctx context.Context, id int64) error

	// MarkIngestDone 标记文档摄取完成，并记录 chunk 数量和 embedding 模型。
	MarkIngestDone(ctx context.Context, id int64, chunkCount int, embeddingModel string) error

	// MarkIngestFailed 标记文档摄取失败，并保存可展示的失败原因。
	MarkIngestFailed(ctx context.Context, id int64, message string) error

	// SoftDelete 软删除文档，使在线查询和检索权限过滤立即不可见。
	SoftDelete(ctx context.Context, id int64) error

	// HardDelete 物理删除文档元数据，通常只用于后台清理或测试。
	HardDelete(ctx context.Context, id int64) error
}
