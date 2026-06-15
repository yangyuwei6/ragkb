package user

import (
	"github.com/gin-gonic/gin"

	"ragkb/internal/handler/shared"
	"ragkb/internal/response"
	"ragkb/internal/service/user"
)

// Handler 处理当前用户信息与租户列表。
type Handler struct {
	users *service.UserService
}

// NewHandler 构造用户 handler。
func NewHandler(users *service.UserService) *Handler {
	return &Handler{users: users}
}

// Me GET /api/v1/users/me
func (h *Handler) Me(c *gin.Context) {
	uid, ok := shared.CurrentUserID(c)
	if !ok {
		response.Error(c, response.CodeUnauthorized, "unauthorized")
		return
	}
	u, err := h.users.Me(c.Request.Context(), uid)
	if err != nil {
		shared.RespondError(c, err)
		return
	}
	response.OK(c, toUserResponse(u))
}

// UpdateMe PATCH /api/v1/users/me
func (h *Handler) UpdateMe(c *gin.Context) {
	uid, ok := shared.CurrentUserID(c)
	if !ok {
		response.Error(c, response.CodeUnauthorized, "unauthorized")
		return
	}
	var req updateMeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, response.CodeBadRequest, err.Error())
		return
	}
	if err := h.users.UpdatePrimaryTenant(c.Request.Context(), uid, req.PrimaryTenant); err != nil {
		shared.RespondError(c, err)
		return
	}
	response.OK(c, gin.H{"primaryTenant": req.PrimaryTenant})
}

// MyTenants GET /api/v1/tenants
func (h *Handler) MyTenants(c *gin.Context) {
	uid, ok := shared.CurrentUserID(c)
	if !ok {
		response.Error(c, response.CodeUnauthorized, "unauthorized")
		return
	}
	tenants, err := h.users.MyTenants(c.Request.Context(), uid)
	if err != nil {
		shared.RespondError(c, err)
		return
	}
	response.OK(c, tenants)
}
