package document

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"ragkb/internal/handler/shared"
	"ragkb/internal/response"
	"ragkb/internal/service/document"
)

// maxPartSize 限制单个分片请求体大小（10MB），防止超大 body 打爆内存。
const maxPartSize = 10 << 20

// Handler 处理分片上传、合并、列表、详情、删除。
type Handler struct {
	docs *service.DocumentService
}

// NewHandler 构造文档 handler。
func NewHandler(docs *service.DocumentService) *Handler {
	return &Handler{docs: docs}
}

// Initiate POST /api/v1/documents
func (h *Handler) Initiate(c *gin.Context) {
	uid, ok := shared.CurrentUserID(c)
	if !ok {
		response.Error(c, response.CodeUnauthorized, "unauthorized")
		return
	}
	var req initiateUploadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, response.CodeBadRequest, err.Error())
		return
	}
	res, err := h.docs.InitiateUpload(c.Request.Context(), service.InitiateParams{
		OwnerID:    uid,
		FileMD5:    req.FileMD5,
		FileName:   req.FileName,
		TotalSize:  req.TotalSize,
		TotalParts: req.TotalParts,
		TenantTag:  req.TenantTag,
		IsPublic:   req.IsPublic,
	})
	if err != nil {
		shared.RespondError(c, err)
		return
	}
	response.OK(c, res)
}

// UploadPart PUT /api/v1/documents/{id}/parts/{partNumber}
func (h *Handler) UploadPart(c *gin.Context) {
	uid, ok := shared.CurrentUserID(c)
	if !ok {
		response.Error(c, response.CodeUnauthorized, "unauthorized")
		return
	}
	docID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.Error(c, response.CodeBadRequest, "invalid document id")
		return
	}
	partNumber, err := strconv.Atoi(c.Param("partNumber"))
	if err != nil {
		response.Error(c, response.CodeBadRequest, "invalid part number")
		return
	}
	totalParts, err := strconv.Atoi(c.Query("totalParts"))
	if err != nil || totalParts <= 0 {
		response.Error(c, response.CodeBadRequest, "missing or invalid totalParts query")
		return
	}

	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxPartSize)
	body := c.Request.Body
	defer body.Close()

	if err := h.docs.UploadPart(c.Request.Context(), uid, docID, partNumber, totalParts, body, c.Request.ContentLength); err != nil {
		shared.RespondError(c, err)
		return
	}
	response.OK(c, gin.H{"partNumber": partNumber, "uploaded": true})
}

// Complete POST /api/v1/documents/{id}/complete
func (h *Handler) Complete(c *gin.Context) {
	uid, ok := shared.CurrentUserID(c)
	if !ok {
		response.Error(c, response.CodeUnauthorized, "unauthorized")
		return
	}
	docID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.Error(c, response.CodeBadRequest, "invalid document id")
		return
	}
	var req completeUploadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, response.CodeBadRequest, err.Error())
		return
	}
	doc, err := h.docs.CompleteUpload(c.Request.Context(), uid, docID, req.TotalParts)
	if err != nil {
		shared.RespondError(c, err)
		return
	}
	response.OK(c, doc)
}

// List GET /api/v1/documents
func (h *Handler) List(c *gin.Context) {
	uid, ok := shared.CurrentUserID(c)
	if !ok {
		response.Error(c, response.CodeUnauthorized, "unauthorized")
		return
	}
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	size, _ := strconv.Atoi(c.DefaultQuery("size", "20"))

	docs, total, err := h.docs.ListDocuments(c.Request.Context(), uid, page, size)
	if err != nil {
		shared.RespondError(c, err)
		return
	}
	response.OK(c, documentListResponse{Items: docs, Total: total, Page: page, Size: size})
}

// Get GET /api/v1/documents/{id}
func (h *Handler) Get(c *gin.Context) {
	uid, ok := shared.CurrentUserID(c)
	if !ok {
		response.Error(c, response.CodeUnauthorized, "unauthorized")
		return
	}
	docID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.Error(c, response.CodeBadRequest, "invalid document id")
		return
	}
	doc, err := h.docs.GetDocument(c.Request.Context(), uid, docID)
	if err != nil {
		shared.RespondError(c, err)
		return
	}
	response.OK(c, doc)
}

// Delete DELETE /api/v1/documents/{id}
func (h *Handler) Delete(c *gin.Context) {
	uid, ok := shared.CurrentUserID(c)
	if !ok {
		response.Error(c, response.CodeUnauthorized, "unauthorized")
		return
	}
	docID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.Error(c, response.CodeBadRequest, "invalid document id")
		return
	}
	if err := h.docs.DeleteDocument(c.Request.Context(), uid, docID); err != nil {
		shared.RespondError(c, err)
		return
	}
	response.OK(c, gin.H{"deleted": true})
}
