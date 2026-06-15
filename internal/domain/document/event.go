package document

const (
	EventTypeFileDelete = "file_delete"
)

// FileIngestEvent 是文件合并完成后发往 Kafka(doc.ingest) 的消息契约。
// worker 消费后据此拉取 merged 文件并执行摄取管线。
type FileIngestEvent struct {
	DocumentID int64  `json:"documentId"`
	FileMD5    string `json:"fileMd5"`
	FileName   string `json:"fileName"`
	FileExt    string `json:"fileExt"`
	MergedKey  string `json:"mergedKey"` // 合并文件在 MinIO 的对象键
	OwnerID    int64  `json:"ownerId"`
	TenantTag  string `json:"tenantTag"`
	IsPublic   bool   `json:"isPublic"`
}

// FileDeleteEvent 是文档删除后发往 Kafka 的异步物理清理消息。
type FileDeleteEvent struct {
	EventType  string `json:"eventType"`
	DocumentID int64  `json:"documentId"`
	FileMD5    string `json:"fileMd5"`
	FileName   string `json:"fileName"`
	MergedKey  string `json:"mergedKey"`
}
