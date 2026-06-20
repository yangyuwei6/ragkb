package document

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	documentdomain "ragkb/internal/domain/document"
	userdomain "ragkb/internal/domain/user"
)

// objectStore 是文档上传流程依赖的对象存储能力。
// 当前由 MinIO 适配层实现，用于保存分片、合并文件和清理临时分片。
type objectStore interface {
	// PutPart 保存单个上传分片。
	PutPart(ctx context.Context, md5 string, index int, r io.Reader, size int64) error

	// ComposeMerged 将多个分片合并为最终文件，并返回合并后对象 key 和文件大小。
	ComposeMerged(ctx context.Context, md5, fileName string, totalParts int) (key string, size int64, err error)

	// RemoveParts 删除已经合并完成的临时分片。
	RemoveParts(ctx context.Context, md5 string, totalParts int) error

	// MergedKey 根据文件信息生成最终合并文件的对象 key。
	MergedKey(md5, fileName string) string
}

// uploadProgress 是分片上传进度存储能力。
// 当前由 Redis 适配层实现，用于断点续传和完整性判断。
type uploadProgress interface {
	// MarkPart 标记某个分片已经上传成功。
	MarkPart(ctx context.Context, md5 string, ownerID int64, index int) error

	// UploadedParts 返回已经上传成功的分片序号列表。
	UploadedParts(ctx context.Context, md5 string, ownerID int64, totalParts int) ([]int, error)

	// AllUploaded 判断所有分片是否都已经上传完成。
	AllUploaded(ctx context.Context, md5 string, ownerID int64, totalParts int) (bool, error)

	// Clear 清理某个文件的上传进度记录。
	Clear(ctx context.Context, md5 string, ownerID int64) error
}

// eventPublisher 是文档服务发布异步事件的能力。
// 当前由 Kafka producer 实现，用于触发摄取和索引清理。
type eventPublisher interface {
	// Publish 发布一条事件消息。
	Publish(ctx context.Context, key, value []byte) error
}

// UploadLimits 定义文档上传的限制条件。
type UploadLimits struct {
	// MaxFileSize 是单文件大小上限，单位为字节。
	MaxFileSize int64
	// AllowedExts 是允许上传的文件扩展名集合。
	AllowedExts map[string]bool
}

// InitiateResult 是发起上传后的返回结果。
type InitiateResult struct {
	// DocumentID 是本次上传对应的文档 ID。
	DocumentID int64 `json:"documentId"`
	// InstantHit 表示是否命中秒传。
	InstantHit bool `json:"instantHit"`
	// UploadedParts 是断点续传场景下已经上传的分片序号。
	UploadedParts []int `json:"uploadedParts"`
}

// InitiateParams 是发起上传用例的输入参数。
type InitiateParams struct {
	OwnerID    int64
	FileMD5    string
	FileName   string
	TotalSize  int64
	TotalParts int
	TenantTag  string
	IsPublic   bool
}

// DocumentService 编排文档上传、查询和删除相关用例。
type DocumentService struct {
	docs     documentdomain.DocumentRepo
	tenants  userdomain.TenantRepo
	objects  objectStore
	progress uploadProgress
	producer eventPublisher
	limits   UploadLimits
}

// NewDocumentService 创建文档应用服务。
func NewDocumentService(
	docs documentdomain.DocumentRepo,
	tenants userdomain.TenantRepo,
	objects objectStore,
	progress uploadProgress,
	producer eventPublisher,
	limits UploadLimits,
) *DocumentService {
	return &DocumentService{
		docs:     docs,
		tenants:  tenants,
		objects:  objects,
		progress: progress,
		producer: producer,
		limits:   limits,
	}
}

// publishIngest 发布文档摄取事件。
// 上传合并成功后，worker 会消费该事件并执行后续解析、切分和索引流程。
func (s *DocumentService) publishIngest(ctx context.Context, doc *documentdomain.Document, mergedKey string) error {
	evt := documentdomain.FileIngestEvent{
		DocumentID: doc.ID,
		FileMD5:    doc.FileMD5,
		FileName:   doc.FileName,
		FileExt:    doc.FileExt,
		MergedKey:  mergedKey,
		OwnerID:    doc.OwnerID,
		TenantTag:  doc.TenantTag,
		IsPublic:   doc.IsPublic,
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	return s.producer.Publish(ctx, []byte(doc.FileMD5), payload)
}

// publishDelete 发布文档删除事件。
// 文档元数据先软删除，索引清理通过异步事件交给 worker 处理。
func (s *DocumentService) publishDelete(ctx context.Context, doc *documentdomain.Document) error {
	evt := documentdomain.FileDeleteEvent{
		EventType:  documentdomain.EventTypeFileDelete,
		DocumentID: doc.ID,
		FileMD5:    doc.FileMD5,
		FileName:   doc.FileName,
		MergedKey:  s.objects.MergedKey(doc.FileMD5, doc.FileName),
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	return s.producer.Publish(ctx, []byte(doc.FileMD5), payload)
}

// fileExt 提取文件扩展名，返回小写且不包含点号。
func fileExt(name string) string {
	i := strings.LastIndex(name, ".")
	if i < 0 || i == len(name)-1 {
		return ""
	}
	return strings.ToLower(name[i+1:])
}

// resolveTenant 解析文档归属租户。
// 未传 tenantTag 时使用用户的默认租户；传入时校验用户是否属于该租户。
func (s *DocumentService) resolveTenant(ctx context.Context, ownerID int64, tenantTag string) (string, error) {
	if tenantTag == "" {
		tag, err := s.tenants.GetPrimaryTag(ctx, ownerID)
		if err != nil {
			if errors.Is(err, userdomain.ErrNotFound) {
				return "", documentdomain.ErrBadRequest
			}
			return "", err
		}
		return tag, nil
	}
	ok, err := s.tenants.IsMember(ctx, ownerID, tenantTag)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", documentdomain.ErrForbidden
	}
	return tenantTag, nil
}

// ownedDoc 查询未删除文档，并校验文档归属当前用户。
func (s *DocumentService) ownedDoc(ctx context.Context, ownerID, docID int64) (*documentdomain.Document, error) {
	doc, err := s.docs.GetByID(ctx, docID)
	if err != nil {
		return nil, err
	}
	if doc.OwnerID != ownerID {
		return nil, documentdomain.ErrNotFound
	}
	return doc, nil
}

// ownedDocAny 查询文档并校验归属，不过滤软删除记录。
// 删除流程需要读取已存在的文档信息来发布索引清理事件。
func (s *DocumentService) ownedDocAny(ctx context.Context, ownerID, docID int64) (*documentdomain.Document, error) {
	doc, err := s.docs.GetByIDAny(ctx, docID)
	if err != nil {
		return nil, err
	}
	if doc.OwnerID != ownerID {
		return nil, documentdomain.ErrNotFound
	}
	return doc, nil
}

// wrapPublishError 为摄取事件发布失败补充文档上下文。
func (s *DocumentService) wrapPublishError(doc *documentdomain.Document, err error) error {
	return fmt.Errorf("publish ingest event for document %d: %w", doc.ID, err)
}

// DeleteDocument 软删除文档，并发布异步索引清理事件。
func (s *DocumentService) DeleteDocument(ctx context.Context, ownerID, docID int64) error {
	doc, err := s.ownedDocAny(ctx, ownerID, docID)
	if err != nil {
		return err
	}
	if err := s.docs.SoftDelete(ctx, docID); err != nil {
		return err
	}
	if err := s.publishDelete(ctx, doc); err != nil {
		return fmt.Errorf("publish delete event for document %d: %w", docID, err)
	}
	return nil
}

// ListDocuments 分页查询当前用户拥有的文档。
func (s *DocumentService) ListDocuments(ctx context.Context, ownerID int64, page, size int) ([]documentdomain.Document, int64, error) {
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 100 {
		size = 20
	}
	return s.docs.ListByOwner(ctx, documentdomain.DocumentListQuery{OwnerID: ownerID, Page: page, Size: size})
}

// GetDocument 查询当前用户拥有的单个文档详情。
func (s *DocumentService) GetDocument(ctx context.Context, ownerID, docID int64) (*documentdomain.Document, error) {
	return s.ownedDoc(ctx, ownerID, docID)
}

// InitiateUpload 发起文档上传。
// 它负责校验上传限制、解析租户、判断秒传/断点续传，并在需要时创建文档记录。
func (s *DocumentService) InitiateUpload(ctx context.Context, p InitiateParams) (*InitiateResult, error) {
	if p.TotalSize > s.limits.MaxFileSize {
		return nil, documentdomain.ErrFileTooLarge
	}
	ext := fileExt(p.FileName)
	if !s.limits.AllowedExts[ext] {
		return nil, documentdomain.ErrUnsupportedType
	}
	if p.TotalParts <= 0 || p.FileMD5 == "" {
		return nil, documentdomain.ErrBadRequest
	}

	tenantTag, err := s.resolveTenant(ctx, p.OwnerID, p.TenantTag)
	if err != nil {
		return nil, err
	}

	existing, err := s.docs.GetByMD5AndOwner(ctx, p.FileMD5, p.OwnerID)
	switch {
	case err == nil:
		if existing.UploadStatus == documentdomain.UploadStatusMerged {
			return &InitiateResult{DocumentID: existing.ID, InstantHit: true}, nil
		}
		parts, perr := s.progress.UploadedParts(ctx, p.FileMD5, p.OwnerID, p.TotalParts)
		if perr != nil {
			return nil, perr
		}
		return &InitiateResult{DocumentID: existing.ID, UploadedParts: parts}, nil
	case errors.Is(err, documentdomain.ErrNotFound):
	default:
		return nil, err
	}

	doc := &documentdomain.Document{
		FileMD5:      p.FileMD5,
		FileName:     p.FileName,
		FileExt:      ext,
		TotalSize:    p.TotalSize,
		UploadStatus: documentdomain.UploadStatusUploading,
		IngestStatus: documentdomain.IngestStatusPending,
		OwnerID:      p.OwnerID,
		TenantTag:    tenantTag,
		IsPublic:     p.IsPublic,
	}
	if err := s.docs.Create(ctx, doc); err != nil {
		return nil, err
	}
	return &InitiateResult{DocumentID: doc.ID, UploadedParts: []int{}}, nil
}

// UploadPart 保存单个分片，并记录该分片已上传。
func (s *DocumentService) UploadPart(ctx context.Context, ownerID, docID int64, partNumber, totalParts int, r io.Reader, size int64) error {
	doc, err := s.ownedDoc(ctx, ownerID, docID)
	if err != nil {
		return err
	}
	if doc.UploadStatus == documentdomain.UploadStatusMerged {
		return nil
	}
	if partNumber < 0 || partNumber >= totalParts {
		return documentdomain.ErrBadRequest
	}
	if err := s.objects.PutPart(ctx, doc.FileMD5, partNumber, r, size); err != nil {
		return err
	}
	return s.progress.MarkPart(ctx, doc.FileMD5, ownerID, partNumber)
}

// CompleteUpload 完成分片上传。
// 它会校验分片完整性、合并对象存储文件、更新文档状态，并发布摄取事件。
func (s *DocumentService) CompleteUpload(ctx context.Context, ownerID, docID int64, totalParts int) (*documentdomain.Document, error) {
	doc, err := s.ownedDoc(ctx, ownerID, docID)
	if err != nil {
		return nil, err
	}
	if doc.UploadStatus == documentdomain.UploadStatusMerged {
		return doc, nil
	}

	allUp, err := s.progress.AllUploaded(ctx, doc.FileMD5, ownerID, totalParts)
	if err != nil {
		return nil, err
	}
	if !allUp {
		return nil, documentdomain.ErrPartsIncomplete
	}

	mergedKey, size, err := s.objects.ComposeMerged(ctx, doc.FileMD5, doc.FileName, totalParts)
	if err != nil {
		return nil, err
	}
	if err := s.docs.MarkMerged(ctx, doc.ID, size); err != nil {
		return nil, err
	}
	doc.UploadStatus = documentdomain.UploadStatusMerged
	doc.TotalSize = size

	_ = s.objects.RemoveParts(ctx, doc.FileMD5, totalParts)
	_ = s.progress.Clear(ctx, doc.FileMD5, ownerID)

	if err := s.publishIngest(ctx, doc, mergedKey); err != nil {
		return doc, s.wrapPublishError(doc, err)
	}
	return doc, nil
}
