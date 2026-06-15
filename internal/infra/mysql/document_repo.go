package mysql

import (
	"context"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"

	documentdomain "ragkb/internal/domain/document"
)

type documentRepo struct {
	db *gorm.DB
}

func NewDocumentRepo(db *gorm.DB) documentdomain.DocumentRepo {
	return &documentRepo{db: db}
}

func (r *documentRepo) Create(ctx context.Context, d *documentdomain.Document) error {
	if err := r.db.WithContext(ctx).Create(d).Error; err != nil {
		if IsDuplicateKey(err) {
			return documentdomain.ErrConflict
		}
		return err
	}
	return nil
}

func (r *documentRepo) GetByID(ctx context.Context, id int64) (*documentdomain.Document, error) {
	var d documentdomain.Document
	err := r.db.WithContext(ctx).
		Where("id = ? AND deleted_at IS NULL", id).
		First(&d).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, documentdomain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (r *documentRepo) GetByIDAny(ctx context.Context, id int64) (*documentdomain.Document, error) {
	var d documentdomain.Document
	err := r.db.WithContext(ctx).
		Where("id = ?", id).
		First(&d).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, documentdomain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (r *documentRepo) GetByMD5AndOwner(ctx context.Context, md5 string, ownerID int64) (*documentdomain.Document, error) {
	var d documentdomain.Document
	err := r.db.WithContext(ctx).
		Where("file_md5 = ? AND owner_id = ? AND deleted_at IS NULL", md5, ownerID).
		First(&d).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, documentdomain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (r *documentRepo) ListByOwner(ctx context.Context, q documentdomain.DocumentListQuery) ([]documentdomain.Document, int64, error) {
	query := r.db.WithContext(ctx).Model(&documentdomain.Document{}).
		Where("owner_id = ? AND deleted_at IS NULL", q.OwnerID)

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var docs []documentdomain.Document
	err := query.Order("created_at DESC").
		Offset((q.Page - 1) * q.Size).
		Limit(q.Size).
		Find(&docs).Error
	if err != nil {
		return nil, 0, err
	}
	return docs, total, nil
}

func (r *documentRepo) MarkMerged(ctx context.Context, id int64, totalSize int64) error {
	return r.db.WithContext(ctx).Model(&documentdomain.Document{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"upload_status": documentdomain.UploadStatusMerged,
			"total_size":    totalSize,
		}).Error
}

func (r *documentRepo) MarkIngestProcessing(ctx context.Context, id int64) error {
	return r.db.WithContext(ctx).Model(&documentdomain.Document{}).
		Where("id = ? AND deleted_at IS NULL", id).
		Updates(map[string]any{
			"ingest_status": documentdomain.IngestStatusProcessing,
			"error_msg":     nil,
		}).Error
}

func (r *documentRepo) MarkIngestDone(ctx context.Context, id int64, chunkCount int, embeddingModel string) error {
	return r.db.WithContext(ctx).Model(&documentdomain.Document{}).
		Where("id = ? AND deleted_at IS NULL", id).
		Updates(map[string]any{
			"ingest_status":   documentdomain.IngestStatusDone,
			"chunk_count":     chunkCount,
			"embedding_model": embeddingModel,
			"error_msg":       nil,
		}).Error
}

func (r *documentRepo) MarkIngestFailed(ctx context.Context, id int64, message string) error {
	message = strings.TrimSpace(message)
	if len(message) > 512 {
		message = message[:512]
	}
	return r.db.WithContext(ctx).Model(&documentdomain.Document{}).
		Where("id = ? AND deleted_at IS NULL", id).
		Updates(map[string]any{
			"ingest_status": documentdomain.IngestStatusFailed,
			"error_msg":     message,
		}).Error
}

func (r *documentRepo) SoftDelete(ctx context.Context, id int64) error {
	return r.db.WithContext(ctx).Model(&documentdomain.Document{}).
		Where("id = ? AND deleted_at IS NULL", id).
		Update("deleted_at", time.Now()).Error
}

func (r *documentRepo) HardDelete(ctx context.Context, id int64) error {
	return r.db.WithContext(ctx).Where("id = ?", id).Delete(&documentdomain.Document{}).Error
}
