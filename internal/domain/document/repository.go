package document

import "context"

type DocumentListQuery struct {
	OwnerID int64
	Page    int
	Size    int
}

type DocumentRepo interface {
	Create(ctx context.Context, d *Document) error
	GetByID(ctx context.Context, id int64) (*Document, error)
	GetByIDAny(ctx context.Context, id int64) (*Document, error)
	GetByMD5AndOwner(ctx context.Context, md5 string, ownerID int64) (*Document, error)
	ListByOwner(ctx context.Context, q DocumentListQuery) ([]Document, int64, error)
	MarkMerged(ctx context.Context, id int64, totalSize int64) error
	MarkIngestProcessing(ctx context.Context, id int64) error
	MarkIngestDone(ctx context.Context, id int64, chunkCount int, embeddingModel string) error
	MarkIngestFailed(ctx context.Context, id int64, message string) error
	SoftDelete(ctx context.Context, id int64) error
	HardDelete(ctx context.Context, id int64) error
}

type ChunkRepo interface {
	DeleteByDocumentID(ctx context.Context, documentID int64) error
	BatchCreate(ctx context.Context, chunks []Chunk) error
	ListByChunkKeys(ctx context.Context, keys []ChunkKey) ([]Chunk, error)
}

type ChunkKey struct {
	DocumentID int64
	Index      int
}
