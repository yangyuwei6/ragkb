package redis

import (
	"context"
	"fmt"
	"time"

	redislib "github.com/redis/go-redis/v9"
)

// RefreshTokenStore 用 Redis 保存有效的 refresh token jti，
// 以支持 refresh token 可吊销。
type RefreshTokenStore struct {
	rdb *redislib.Client
}

// NewRefreshTokenStore 构造 refresh token 存储。
func NewRefreshTokenStore(rdb *redislib.Client) *RefreshTokenStore {
	return &RefreshTokenStore{rdb: rdb}
}

func refreshKey(jti string) string {
	return "refresh:" + jti
}

// Save 记录一个仍然有效的 refresh token。
func (s *RefreshTokenStore) Save(ctx context.Context, jti string, userID int64, ttl time.Duration) error {
	return s.rdb.Set(ctx, refreshKey(jti), userID, ttl).Err()
}

// Exists 判断 refresh token 是否仍有效。
func (s *RefreshTokenStore) Exists(ctx context.Context, jti string) (bool, error) {
	n, err := s.rdb.Exists(ctx, refreshKey(jti)).Result()
	if err != nil {
		return false, fmt.Errorf("check refresh token: %w", err)
	}
	return n > 0, nil
}

// Revoke 吊销一个 refresh token。
func (s *RefreshTokenStore) Revoke(ctx context.Context, jti string) error {
	return s.rdb.Del(ctx, refreshKey(jti)).Err()
}
