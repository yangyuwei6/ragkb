package response

import "github.com/gin-gonic/gin"

const (
	CodeOK           = 0
	CodeBadRequest   = 400
	CodeUnauthorized = 401
	CodeForbidden    = 403
	CodeNotFound     = 404
	CodeConflict     = 409
	CodeInternal     = 500
)

type envelope struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// OK 返回统一成功响应。
func OK(c *gin.Context, data any) {
	c.JSON(200, envelope{
		Code:    CodeOK,
		Message: "ok",
		Data:    data,
	})
}

// Error 返回统一错误响应。
func Error(c *gin.Context, code int, message string) {
	httpStatus := 500
	switch code {
	case CodeBadRequest:
		httpStatus = 400
	case CodeUnauthorized:
		httpStatus = 401
	case CodeForbidden:
		httpStatus = 403
	case CodeNotFound:
		httpStatus = 404
	case CodeConflict:
		httpStatus = 409
	}

	c.JSON(httpStatus, envelope{
		Code:    code,
		Message: message,
	})
}
