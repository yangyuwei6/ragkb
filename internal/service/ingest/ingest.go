package ingest

import "context"

// Service 编排离线摄取流程：拉取文件、解析文本、切分 chunk、向量化并写入索引。
type Service struct{}

// NewService 创建摄取应用服务。
func NewService() *Service {
	return &Service{}
}

// DocumentIngestCommand 是 worker 消费到文档摄取消息后的应用层命令。
type DocumentIngestCommand struct {
	DocumentID int64
	FileKey    string
}

// IngestDocument 执行单个文档的完整摄取流程。
func (s *Service) IngestDocument(ctx context.Context, cmd DocumentIngestCommand) error {
	return nil
}

// DocumentDeleteCommand 是删除文档索引时使用的应用层命令。
type DocumentDeleteCommand struct {
	DocumentID int64
}

// DeleteDocumentIndexes 清理文档在检索索引中的数据。
func (s *Service) DeleteDocumentIndexes(ctx context.Context, cmd DocumentDeleteCommand) error {
	return nil
}
