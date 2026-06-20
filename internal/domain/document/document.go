package document

import "time"

const (
	UploadStatusUploading = 0
	UploadStatusMerged    = 1
)

const (
	IngestStatusPending    = 0
	IngestStatusProcessing = 1
	IngestStatusDone       = 2
	IngestStatusFailed     = 3
)

type Document struct {
	ID             int64      `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	FileMD5        string     `gorm:"column:file_md5" json:"fileMd5"`
	FileName       string     `gorm:"column:file_name" json:"fileName"`
	FileExt        string     `gorm:"column:file_ext" json:"fileExt"`
	TotalSize      int64      `gorm:"column:total_size" json:"totalSize"`
	UploadStatus   int        `gorm:"column:upload_status" json:"uploadStatus"`
	IngestStatus   int        `gorm:"column:ingest_status" json:"ingestStatus"`
	ChunkCount     int        `gorm:"column:chunk_count" json:"chunkCount"`
	EmbeddingModel *string    `gorm:"column:embedding_model" json:"embeddingModel,omitempty"`
	OwnerID        int64      `gorm:"column:owner_id" json:"ownerId"`
	TenantTag      string     `gorm:"column:tenant_tag" json:"tenantTag"`
	IsPublic       bool       `gorm:"column:is_public" json:"isPublic"`
	ErrorMsg       *string    `gorm:"column:error_msg" json:"errorMsg,omitempty"`
	CreatedAt      time.Time  `gorm:"column:created_at" json:"createdAt"`
	UpdatedAt      time.Time  `gorm:"column:updated_at" json:"updatedAt"`
	DeletedAt      *time.Time `gorm:"column:deleted_at" json:"deletedAt,omitempty"`
}

func (Document) TableName() string { return "documents" }
