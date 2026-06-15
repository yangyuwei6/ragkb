package document

import documentdomain "ragkb/internal/domain/document"

// ---- document DTO ----

type initiateUploadRequest struct {
	FileMD5    string `json:"fileMd5" binding:"required"`
	FileName   string `json:"fileName" binding:"required"`
	TotalSize  int64  `json:"totalSize" binding:"required"`
	TotalParts int    `json:"totalParts" binding:"required"`
	TenantTag  string `json:"tenantTag"`
	IsPublic   bool   `json:"isPublic"`
}

type completeUploadRequest struct {
	TotalParts int `json:"totalParts" binding:"required"`
}

type documentListResponse struct {
	Items []documentdomain.Document `json:"items"`
	Total int64                     `json:"total"`
	Page  int                       `json:"page"`
	Size  int                       `json:"size"`
}
