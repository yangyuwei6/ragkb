package search

import "context"

// Service 编排在线检索流程：权限过滤、混合召回、RRF 融合和 rerank。
type Service struct{}

// NewService 创建检索应用服务。
func NewService() *Service {
	return &Service{}
}

// Query 表示一次检索请求。
type Query struct {
	UserID    int64
	TenantTag string
	Text      string
	TopK      int
}

// Result 是检索服务返回给问答链路或调试接口的 chunk 结果。
type Result struct {
	ChunkID    int64
	DocumentID int64
	Score      float64
	Content    string
}

// Search 执行一次 RAG 检索。
func (s *Service) Search(ctx context.Context, q Query) ([]Result, error) {
	return nil, nil
}
