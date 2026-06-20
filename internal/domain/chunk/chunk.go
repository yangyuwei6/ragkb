package chunk

import "time"

// Chunk 表示文档切分后的最小检索单元，也是 ES/Qdrant 索引的业务来源。
type Chunk struct {
	ID         int64     `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	DocumentID int64     `gorm:"column:document_id" json:"documentId"`
	ChunkIndex int       `gorm:"column:chunk_index" json:"chunkIndex"`
	Content    string    `gorm:"column:content" json:"content"`
	TokenCount int       `gorm:"column:token_count" json:"tokenCount"`
	CharStart  *int      `gorm:"column:char_start" json:"charStart,omitempty"`
	CharEnd    *int      `gorm:"column:char_end" json:"charEnd,omitempty"`
	CreatedAt  time.Time `gorm:"column:created_at" json:"createdAt"`
}

func (Chunk) TableName() string { return "chunks" }
