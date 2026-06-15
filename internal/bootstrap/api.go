package bootstrap

import (
	"fmt"
	"net/http"

	"go.uber.org/zap"

	"ragkb/internal/config"
	authhandler "ragkb/internal/handler/auth"
	documenthandler "ragkb/internal/handler/document"
	userhandler "ragkb/internal/handler/user"
	jwtinfra "ragkb/internal/infra/jwt"
	"ragkb/internal/infra/kafka"
	"ragkb/internal/infra/minio"
	"ragkb/internal/infra/mysql"
	"ragkb/internal/infra/redis"
	"ragkb/internal/server"
	authservice "ragkb/internal/service/auth"
	documentservice "ragkb/internal/service/document"
	userservice "ragkb/internal/service/user"
)

// API holds resources owned by the HTTP process.
type API struct {
	Server   *http.Server
	cleanups []func()
}

// NewAPI initializes dependencies for auth, user, and upload modules.
func NewAPI(cfg *config.Config, logger *zap.Logger) (*API, error) {
	db, err := mysql.New(cfg.MySQL)
	if err != nil {
		return nil, fmt.Errorf("init mysql: %w", err)
	}

	rdb, err := redis.New(cfg.Redis)
	if err != nil {
		return nil, fmt.Errorf("init redis: %w", err)
	}

	minioClient, err := minio.New(cfg.MinIO)
	if err != nil {
		return nil, fmt.Errorf("init minio: %w", err)
	}

	tokens := jwtinfra.NewTokenManager(cfg.JWT)
	userRepo := mysql.NewUserRepo(db)
	tenantRepo := mysql.NewTenantRepo(db)
	docRepo := mysql.NewDocumentRepo(db)
	refreshStore := redis.NewRefreshTokenStore(rdb)
	uploadProgress := redis.NewUploadProgress(rdb)
	objectStore := minio.NewObjectStore(minioClient, cfg.MinIO.Bucket)
	producer := kafka.NewProducer(cfg.Kafka)

	allowed := make(map[string]bool, len(cfg.Upload.AllowedExts))
	for _, ext := range cfg.Upload.AllowedExts {
		allowed[ext] = true
	}
	uploadLimits := documentservice.UploadLimits{
		MaxFileSize: int64(cfg.Upload.MaxFileSizeMB) << 20,
		AllowedExts: allowed,
	}

	authService := authservice.NewAuthService(userRepo, tenantRepo, tokens, refreshStore)
	userService := userservice.NewUserService(userRepo, tenantRepo)
	docService := documentservice.NewDocumentService(docRepo, tenantRepo, objectStore, uploadProgress, producer, uploadLimits)

	mode := "debug"
	if cfg.Log.Mode == "prod" {
		mode = "release"
	}
	engine := server.New(server.Options{
		Mode:     mode,
		Logger:   logger,
		Tokens:   tokens,
		Auth:     authhandler.NewHandler(authService),
		User:     userhandler.NewHandler(userService),
		Document: documenthandler.NewHandler(docService),
	})

	return &API{
		Server: &http.Server{
			Addr:    cfg.HTTP.Addr,
			Handler: engine,
		},
		cleanups: []func(){
			func() { _ = producer.Close() },
			func() { _ = rdb.Close() },
		},
	}, nil
}

// Close releases resources owned by the HTTP process.
func (a *API) Close() {
	for _, cleanup := range a.cleanups {
		cleanup()
	}
}
