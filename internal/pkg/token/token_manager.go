package token

import (
	"errors"
	"fmt"
	"time"

	gjwt "github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"ragkb/internal/config"
)

const (
	typeAccess  = "access"
	typeRefresh = "refresh"
)

// ErrInvalidToken 表示 token 解析失败、过期或类型不符。
var ErrInvalidToken = errors.New("invalid or expired token")

// Claims 是自定义 JWT 载荷。
type Claims struct {
	UserID int64  `json:"uid"`
	Type   string `json:"typ"`
	gjwt.RegisteredClaims
}

// TokenManager 负责签发与解析 JWT。
type TokenManager struct {
	secret        []byte
	accessExpire  time.Duration
	refreshExpire time.Duration
}

// NewTokenManager 从配置构造 TokenManager。
func NewTokenManager(cfg config.JWTConfig) *TokenManager {
	return &TokenManager{
		secret:        []byte(cfg.Secret),
		accessExpire:  time.Duration(cfg.ExpireHours) * time.Hour,
		refreshExpire: time.Duration(cfg.RefreshExpireHours) * time.Hour,
	}
}

func (m *TokenManager) AccessTTL() time.Duration  { return m.accessExpire }
func (m *TokenManager) RefreshTTL() time.Duration { return m.refreshExpire }

// GenerateAccessToken 签发短期 access token。
func (m *TokenManager) GenerateAccessToken(userID int64) (string, error) {
	return m.sign(userID, typeAccess, "", m.accessExpire)
}

// GenerateRefreshToken 签发长期 refresh token，并返回 jti。
func (m *TokenManager) GenerateRefreshToken(userID int64) (tok, jti string, err error) {
	jti = uuid.NewString()
	tok, err = m.sign(userID, typeRefresh, jti, m.refreshExpire)
	return tok, jti, err
}

func (m *TokenManager) sign(userID int64, typ, jti string, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := Claims{
		UserID: userID,
		Type:   typ,
		RegisteredClaims: gjwt.RegisteredClaims{
			ID:        jti,
			IssuedAt:  gjwt.NewNumericDate(now),
			ExpiresAt: gjwt.NewNumericDate(now.Add(ttl)),
		},
	}
	tok := gjwt.NewWithClaims(gjwt.SigningMethodHS256, claims)
	return tok.SignedString(m.secret)
}

// ParseAccessToken 解析并校验 access token。
func (m *TokenManager) ParseAccessToken(raw string) (*Claims, error) {
	return m.parse(raw, typeAccess)
}

// ParseRefreshToken 解析并校验 refresh token。
func (m *TokenManager) ParseRefreshToken(raw string) (*Claims, error) {
	return m.parse(raw, typeRefresh)
}

func (m *TokenManager) parse(raw, wantType string) (*Claims, error) {
	claims := &Claims{}
	_, err := gjwt.ParseWithClaims(raw, claims, func(t *gjwt.Token) (any, error) {
		if _, ok := t.Method.(*gjwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return m.secret, nil
	})
	if err != nil {
		return nil, ErrInvalidToken
	}
	if claims.Type != wantType {
		return nil, ErrInvalidToken
	}
	return claims, nil
}
