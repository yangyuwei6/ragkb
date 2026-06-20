package document

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"ragkb/internal/handler/shared"
	"ragkb/internal/response"
	documentservice "ragkb/internal/service/document"
)

const maxPartSize = 10 << 20

// Handler 处理文档上传、合并、查询和删除相关的 HTTP 请求。
type Handler struct {
	docs *documentservice.DocumentService
}

// NewHandler 创建文档 HTTP handler。
func NewHandler(docs *documentservice.DocumentService) *Handler {
	return &Handler{docs: docs}
}

// Initiate 发起一次分片上传，并返回已上传分片或秒传结果。
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
	res, err := h.docs.InitiateUpload(c.Request.Context(), documentservice.InitiateParams{
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

// UploadPart 上传单个分片。partNumber 从 0 开始，重复上传同一分片应保持幂等。
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

// Complete 合并所有分片，并发布文档摄取事件。
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

// List 返回当前用户可见的文档列表。
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

// Get 返回当前用户拥有的单个文档详情。
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

// Delete 软删除文档，并发布索引清理事件。
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
