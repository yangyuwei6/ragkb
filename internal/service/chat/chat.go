package chat

import "context"

// Service 编排问答流程：读取会话、检索上下文、调用 LLM，并持久化消息。
type Service struct{}

// NewService 创建问答应用服务。
func NewService() *Service {
	return &Service{}
}

// AskCommand 表示用户在某个会话中发起的一次提问。
type AskCommand struct {
	UserID         int64
	ConversationID int64
	Query          string
}

// Answer 表示一次问答完成后的结果。
type Answer struct {
	MessageID int64
	Content   string
}

// Ask 执行一次完整的 RAG 问答。
func (s *Service) Ask(ctx context.Context, cmd AskCommand) (*Answer, error) {
	return nil, nil
}
