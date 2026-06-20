package shared

import (
	"errors"

	"github.com/gin-gonic/gin"

	documentdomain "ragkb/internal/domain/document"
	userdomain "ragkb/internal/domain/user"
	"ragkb/internal/pkg/token"
	"ragkb/internal/response"
)

const ContextUserID = "currentUserID"

// CurrentUserID reads the authenticated user ID from gin.Context.
func CurrentUserID(c *gin.Context) (int64, bool) {
	v, ok := c.Get(ContextUserID)
	if !ok {
		return 0, false
	}
	uid, ok := v.(int64)
	return uid, ok
}

// RespondError maps domain errors to the shared HTTP response envelope.
func RespondError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, documentdomain.ErrBadRequest):
		response.Error(c, response.CodeBadRequest, err.Error())
	case errors.Is(err, userdomain.ErrForbidden),
		errors.Is(err, documentdomain.ErrForbidden):
		response.Error(c, response.CodeForbidden, err.Error())
	case errors.Is(err, userdomain.ErrNotFound),
		errors.Is(err, documentdomain.ErrNotFound):
		response.Error(c, response.CodeNotFound, err.Error())
	case errors.Is(err, userdomain.ErrAlreadyExists),
		errors.Is(err, userdomain.ErrConflict),
		errors.Is(err, documentdomain.ErrConflict):
		response.Error(c, response.CodeConflict, err.Error())
	case errors.Is(err, userdomain.ErrInvalidCredentials),
		errors.Is(err, userdomain.ErrInvalidToken),
		errors.Is(err, token.ErrInvalidToken):
		response.Error(c, response.CodeUnauthorized, err.Error())
	case errors.Is(err, documentdomain.ErrFileTooLarge),
		errors.Is(err, documentdomain.ErrUnsupportedType),
		errors.Is(err, documentdomain.ErrPartsIncomplete):
		response.Error(c, response.CodeBadRequest, err.Error())
	default:
		response.Error(c, response.CodeInternal, "internal server error")
	}
}
