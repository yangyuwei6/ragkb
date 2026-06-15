package service

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

type objectStore interface {
	PutPart(ctx context.Context, md5 string, index int, r io.Reader, size int64) error
	ComposeMerged(ctx context.Context, md5, fileName string, totalParts int) (key string, size int64, err error)
	RemoveParts(ctx context.Context, md5 string, totalParts int) error
	MergedKey(md5, fileName string) string
}

type uploadProgress interface {
	MarkPart(ctx context.Context, md5 string, ownerID int64, index int) error
	UploadedParts(ctx context.Context, md5 string, ownerID int64, totalParts int) ([]int, error)
	AllUploaded(ctx context.Context, md5 string, ownerID int64, totalParts int) (bool, error)
	Clear(ctx context.Context, md5 string, ownerID int64) error
}

type eventPublisher interface {
	Publish(ctx context.Context, key, value []byte) error
}

type UploadLimits struct {
	MaxFileSize int64
	AllowedExts map[string]bool
}

type InitiateResult struct {
	DocumentID    int64 `json:"documentId"`
	InstantHit    bool  `json:"instantHit"`
	UploadedParts []int `json:"uploadedParts"`
}

type InitiateParams struct {
	OwnerID    int64
	FileMD5    string
	FileName   string
	TotalSize  int64
	TotalParts int
	TenantTag  string
	IsPublic   bool
}

type DocumentService struct {
	docs     documentdomain.DocumentRepo
	tenants  userdomain.TenantRepo
	objects  objectStore
	progress uploadProgress
	producer eventPublisher
	limits   UploadLimits
}

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

func fileExt(name string) string {
	i := strings.LastIndex(name, ".")
	if i < 0 || i == len(name)-1 {
		return ""
	}
	return strings.ToLower(name[i+1:])
}

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

func (s *DocumentService) wrapPublishError(doc *documentdomain.Document, err error) error {
	return fmt.Errorf("publish ingest event for document %d: %w", doc.ID, err)
}

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

func (s *DocumentService) ListDocuments(ctx context.Context, ownerID int64, page, size int) ([]documentdomain.Document, int64, error) {
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 100 {
		size = 20
	}
	return s.docs.ListByOwner(ctx, documentdomain.DocumentListQuery{OwnerID: ownerID, Page: page, Size: size})
}

func (s *DocumentService) GetDocument(ctx context.Context, ownerID, docID int64) (*documentdomain.Document, error) {
	return s.ownedDoc(ctx, ownerID, docID)
}

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
