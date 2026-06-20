package server

import (
	"strings"

	"github.com/gin-gonic/gin"

	"ragkb/internal/handler/shared"
	"ragkb/internal/pkg/token"
	"ragkb/internal/response"
)

// JWTAuth 是鉴权中间件：解析 Authorization: Bearer <token> 里的 access token。
func JWTAuth(tm *token.TokenManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		raw := c.GetHeader("Authorization")
		token, ok := bearerToken(raw)
		if !ok {
			response.Error(c, response.CodeUnauthorized, "missing or malformed Authorization header")
			c.Abort()
			return
		}
		claims, err := tm.ParseAccessToken(token)
		if err != nil {
			response.Error(c, response.CodeUnauthorized, "invalid or expired token")
			c.Abort()
			return
		}
		c.Set(shared.ContextUserID, claims.UserID)
		c.Next()
	}
}

func bearerToken(header string) (string, bool) {
	const prefix = "Bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	token := strings.TrimSpace(header[len(prefix):])
	return token, token != ""
}
