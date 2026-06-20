package chunk

import "context"

// Repository 定义 chunk 领域的持久化能力。
// 具体实现可以是 MySQL，索引库写入应放在应用层或基础设施适配层。
type Repository interface {
	// DeleteByDocumentID 删除某个文档下的所有 chunk，常用于重新摄取前清理旧数据。
	DeleteByDocumentID(ctx context.Context, documentID int64) error

	// BatchCreate 批量写入 chunk，供摄取流程在切分完成后一次性落库。
	BatchCreate(ctx context.Context, chunks []Chunk) error

	// ListByKeys 根据文档 ID 和 chunk 序号批量回查 chunk。
	// 问答引用、索引重建、检索结果补全都可能用到它。
	ListByKeys(ctx context.Context, keys []Key) ([]Chunk, error)
}

// Key 是 chunk 在业务上的稳定定位方式。
// document_id + chunk_index 与数据库唯一索引保持一致。
type Key struct {
	DocumentID int64
	Index      int
}
