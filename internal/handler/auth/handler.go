package auth

import (
	"github.com/gin-gonic/gin"

	"ragkb/internal/handler/shared"
	"ragkb/internal/response"
	"ragkb/internal/service/auth"
)

// Handler 处理注册、登录、刷新、登出。
type Handler struct {
	auth *service.AuthService
}

// NewHandler 构造认证 handler。
func NewHandler(auth *service.AuthService) *Handler {
	return &Handler{auth: auth}
}

// Register POST /api/v1/auth/register
func (h *Handler) Register(c *gin.Context) {
	var req registerRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, response.CodeBadRequest, err.Error())
		return
	}
	u, err := h.auth.Register(c.Request.Context(), req.Username, req.Password)
	if err != nil {
		shared.RespondError(c, err)
		return
	}
	response.OK(c, toUserResponse(u))
}

// Login POST /api/v1/auth/login
func (h *Handler) Login(c *gin.Context) {
	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, response.CodeBadRequest, err.Error())
		return
	}
	u, pair, err := h.auth.Login(c.Request.Context(), req.Username, req.Password)
	if err != nil {
		shared.RespondError(c, err)
		return
	}
	response.OK(c, tokenResponse{
		AccessToken:  pair.AccessToken,
		RefreshToken: pair.RefreshToken,
		User:         toUserResponse(u),
	})
}

// Refresh POST /api/v1/auth/refresh
func (h *Handler) Refresh(c *gin.Context) {
	var req refreshRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, response.CodeBadRequest, err.Error())
		return
	}
	access, err := h.auth.Refresh(c.Request.Context(), req.RefreshToken)
	if err != nil {
		shared.RespondError(c, err)
		return
	}
	response.OK(c, tokenResponse{AccessToken: access})
}

// Logout POST /api/v1/auth/logout
func (h *Handler) Logout(c *gin.Context) {
	var req logoutRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, response.CodeBadRequest, err.Error())
		return
	}
	if err := h.auth.Logout(c.Request.Context(), req.RefreshToken); err != nil {
		shared.RespondError(c, err)
		return
	}
	response.OK(c, gin.H{"loggedOut": true})
}
