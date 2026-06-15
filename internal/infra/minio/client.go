package minio

import (
	"context"
	"fmt"
	"time"

	miniolib "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"ragkb/internal/config"
)

// New 创建 MinIO 客户端，验证连通性并确保 bucket 存在。
func New(cfg config.MinIOConfig) (*miniolib.Client, error) {
	client, err := miniolib.New(cfg.Endpoint, &miniolib.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("create minio client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("check minio bucket: %w", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, cfg.Bucket, miniolib.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("create minio bucket %q: %w", cfg.Bucket, err)
		}
	}
	return client, nil
}
