package server

import (
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	authhandler "ragkb/internal/handler/auth"
	documenthandler "ragkb/internal/handler/document"
	userhandler "ragkb/internal/handler/user"
	"ragkb/internal/pkg/token"
)

// Options groups dependencies required by the HTTP router.
type Options struct {
	Mode   string
	Logger *zap.Logger

	Tokens   *token.TokenManager
	Auth     *authhandler.Handler
	User     *userhandler.Handler
	Document *documenthandler.Handler
}

// New builds the Gin engine and registers API routes.
func New(opts Options) *gin.Engine {
	if opts.Mode == "release" {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(RequestLogger(opts.Logger))

	api := r.Group("/api/v1")
	registerRoutes(api, opts)

	return r
}

func registerRoutes(api *gin.RouterGroup, opts Options) {
	if opts.Auth != nil {
		a := api.Group("/auth")
		a.POST("/register", opts.Auth.Register)
		a.POST("/login", opts.Auth.Login)
		a.POST("/refresh", opts.Auth.Refresh)
		a.POST("/logout", opts.Auth.Logout)
	}

	if opts.Tokens == nil {
		return
	}
	authed := api.Group("")
	authed.Use(JWTAuth(opts.Tokens))

	if opts.User != nil {
		authed.GET("/users/me", opts.User.Me)
		authed.PATCH("/users/me", opts.User.UpdateMe)
		authed.GET("/tenants", opts.User.MyTenants)
	}

	if opts.Document != nil {
		authed.POST("/documents", opts.Document.Initiate)
		authed.PUT("/documents/:id/parts/:partNumber", opts.Document.UploadPart)
		authed.POST("/documents/:id/complete", opts.Document.Complete)
		authed.GET("/documents", opts.Document.List)
		authed.GET("/documents/:id", opts.Document.Get)
		authed.DELETE("/documents/:id", opts.Document.Delete)
	}
}
