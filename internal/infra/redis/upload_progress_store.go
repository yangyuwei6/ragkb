package redis

import (
	"context"
	"fmt"
	"time"

	redislib "github.com/redis/go-redis/v9"
)

const uploadProgressTTL = 24 * time.Hour

// UploadProgress 用 Redis bitmap 记录文件上传分片进度：
// key = upload:{md5}:{ownerID}，第 i 位表示第 i 个分片是否已上传。
type UploadProgress struct {
	rdb *redislib.Client
}

// NewUploadProgress 构造分片上传进度跟踪器。
func NewUploadProgress(rdb *redislib.Client) *UploadProgress {
	return &UploadProgress{rdb: rdb}
}

func progressKey(md5 string, ownerID int64) string {
	return fmt.Sprintf("upload:%s:%d", md5, ownerID)
}

// MarkPart 标记某个分片已上传，并刷新 TTL。
func (p *UploadProgress) MarkPart(ctx context.Context, md5 string, ownerID int64, index int) error {
	key := progressKey(md5, ownerID)
	pipe := p.rdb.TxPipeline()
	pipe.SetBit(ctx, key, int64(index), 1)
	pipe.Expire(ctx, key, uploadProgressTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// UploadedParts 返回已上传分片索引列表，供断点续传使用。
func (p *UploadProgress) UploadedParts(ctx context.Context, md5 string, ownerID int64, totalParts int) ([]int, error) {
	key := progressKey(md5, ownerID)
	uploaded := make([]int, 0, totalParts)
	for i := 0; i < totalParts; i++ {
		bit, err := p.rdb.GetBit(ctx, key, int64(i)).Result()
		if err != nil {
			return nil, err
		}
		if bit == 1 {
			uploaded = append(uploaded, i)
		}
	}
	return uploaded, nil
}

// AllUploaded 判断 [0, totalParts) 是否已全部上传完成。
func (p *UploadProgress) AllUploaded(ctx context.Context, md5 string, ownerID int64, totalParts int) (bool, error) {
	uploaded, err := p.UploadedParts(ctx, md5, ownerID, totalParts)
	if err != nil {
		return false, err
	}
	return len(uploaded) == totalParts, nil
}

// Clear 清除某个文件的上传进度记录。
func (p *UploadProgress) Clear(ctx context.Context, md5 string, ownerID int64) error {
	return p.rdb.Del(ctx, progressKey(md5, ownerID)).Err()
}
