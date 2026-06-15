package user

import userdomain "ragkb/internal/domain/user"

// ---- user DTO ----

type updateMeRequest struct {
	PrimaryTenant string `json:"primaryTenant" binding:"required"`
}

type userResponse struct {
	ID            int64  `json:"id"`
	Username      string `json:"username"`
	Role          string `json:"role"`
	PrimaryTenant string `json:"primaryTenant,omitempty"`
}

func toUserResponse(u *userdomain.User) *userResponse {
	return &userResponse{
		ID:            u.ID,
		Username:      u.Username,
		Role:          u.Role,
		PrimaryTenant: u.PrimaryTenant,
	}
}
