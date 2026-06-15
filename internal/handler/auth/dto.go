package auth

import userdomain "ragkb/internal/domain/user"

// ---- auth DTO ----

type registerRequest struct {
	Username string `json:"username" binding:"required,min=3,max=64"`
	Password string `json:"password" binding:"required,min=6,max=72"`
}

type loginRequest struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

type refreshRequest struct {
	RefreshToken string `json:"refreshToken" binding:"required"`
}

type logoutRequest struct {
	RefreshToken string `json:"refreshToken" binding:"required"`
}

type tokenResponse struct {
	AccessToken  string        `json:"accessToken"`
	RefreshToken string        `json:"refreshToken,omitempty"`
	User         *userResponse `json:"user,omitempty"`
}

type userResponse struct {
	ID            int64  `json:"id"`
	Username      string `json:"username"`
	Role          string `json:"role"`
	PrimaryTenant string `json:"primaryTenant,omitempty"`
}

// toUserResponse 把 user.User 转成 HTTP 响应 DTO。
func toUserResponse(u *userdomain.User) *userResponse {
	return &userResponse{
		ID:            u.ID,
		Username:      u.Username,
		Role:          u.Role,
		PrimaryTenant: u.PrimaryTenant,
	}
}
