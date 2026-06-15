package minio

import (
	"context"
	"fmt"
	"io"

	miniolib "github.com/minio/minio-go/v7"
)

func partKey(md5 string, index int) string {
	return fmt.Sprintf("chunks/%s/%d", md5, index)
}

// MergedKey 返回合并文件的对象键，供 service 写入 Kafka 事件。
func MergedKey(md5, fileName string) string {
	return fmt.Sprintf("merged/%s/%s", md5, fileName)
}

// ObjectStore 封装 MinIO 的分片上传、合并与清理操作。
type ObjectStore struct {
	client *miniolib.Client
	bucket string
}

// NewObjectStore 构造对象存储包装器。
func NewObjectStore(client *miniolib.Client, bucket string) *ObjectStore {
	return &ObjectStore{client: client, bucket: bucket}
}

func (s *ObjectStore) MergedKey(md5, fileName string) string {
	return MergedKey(md5, fileName)
}

// PutPart 上传单个分片到 chunks/{md5}/{index}。
// PUT 幂等：重传同一分片会覆盖旧对象。
func (s *ObjectStore) PutPart(ctx context.Context, md5 string, index int, r io.Reader, size int64) error {
	_, err := s.client.PutObject(ctx, s.bucket, partKey(md5, index), r, size, miniolib.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return fmt.Errorf("put part %d: %w", index, err)
	}
	return nil
}

// GetObject 读取合并后的原始文件，供 worker 拉取并交给 Tika 解析。
func (s *ObjectStore) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, miniolib.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get object %q: %w", key, err)
	}
	return obj, nil
}

// RemoveObject 删除单个对象，用于异步物理清理合并文件。
func (s *ObjectStore) RemoveObject(ctx context.Context, key string) error {
	if err := s.client.RemoveObject(ctx, s.bucket, key, miniolib.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("remove object %q: %w", key, err)
	}
	return nil
}

// ComposeMerged 按顺序合并 [0, totalParts) 的全部分片。
func (s *ObjectStore) ComposeMerged(ctx context.Context, md5, fileName string, totalParts int) (key string, size int64, err error) {
	srcs := make([]miniolib.CopySrcOptions, totalParts)
	for i := 0; i < totalParts; i++ {
		srcs[i] = miniolib.CopySrcOptions{Bucket: s.bucket, Object: partKey(md5, i)}
	}
	dst := miniolib.CopyDestOptions{Bucket: s.bucket, Object: MergedKey(md5, fileName)}

	info, err := s.client.ComposeObject(ctx, dst, srcs...)
	if err != nil {
		return "", 0, fmt.Errorf("compose merged object: %w", err)
	}
	return dst.Object, info.Size, nil
}

// RemoveParts 删除某个文件的全部临时分片对象。
func (s *ObjectStore) RemoveParts(ctx context.Context, md5 string, totalParts int) error {
	for i := 0; i < totalParts; i++ {
		if err := s.client.RemoveObject(ctx, s.bucket, partKey(md5, i), miniolib.RemoveObjectOptions{}); err != nil {
			return fmt.Errorf("remove part %d: %w", i, err)
		}
	}
	return nil
}
