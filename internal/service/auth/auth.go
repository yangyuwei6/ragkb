package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	userdomain "ragkb/internal/domain/user"
	"ragkb/internal/pkg/token"
)

// refreshStore 是 auth service 依赖的 refresh token 存储契约。
// 定义在 service 包内，便于测试替换。
type refreshStore interface {
	Save(ctx context.Context, jti string, userID int64, ttl time.Duration) error
	Exists(ctx context.Context, jti string) (bool, error)
	Revoke(ctx context.Context, jti string) error
}

// TokenPair 是一次签发返回的 access + refresh token。
type TokenPair struct {
	AccessToken  string
	RefreshToken string
}

// AuthService 编排注册、登录、刷新、登出。
type AuthService struct {
	users   userdomain.UserRepo
	tenants userdomain.TenantRepo
	tokens  *token.TokenManager
	refresh refreshStore
}

// NewAuthService 构造认证服务。
func NewAuthService(users userdomain.UserRepo, tenants userdomain.TenantRepo, tokens *token.TokenManager, refresh refreshStore) *AuthService {
	return &AuthService{users: users, tenants: tenants, tokens: tokens, refresh: refresh}
}

// Register 创建用户：bcrypt 加密密码 -> 建用户 -> 自动建立私人租户并设为默认。
func (s *AuthService) Register(ctx context.Context, username, password string) (*userdomain.User, error) {
	hash, err := userdomain.HashPassword(password)
	if err != nil {
		return nil, err
	}

	u := &userdomain.User{
		Username: username,
		Password: hash,
		Role:     userdomain.RoleUser,
	}
	if err := s.users.Create(ctx, u); err != nil {
		if errors.Is(err, userdomain.ErrConflict) {
			return nil, userdomain.ErrAlreadyExists
		}
		return nil, err
	}

	tenant := &userdomain.Tenant{
		Tag:       "PRIVATE_" + username,
		Name:      fmt.Sprintf("%s 的个人空间", username),
		CreatedBy: u.ID,
	}
	if err := s.tenants.CreateWithMembership(ctx, tenant, true); err != nil {
		return nil, err
	}
	u.PrimaryTenant = tenant.Tag
	return u, nil
}

// Login 校验用户名密码，签发 token 对，并把 refresh jti 登记进 Redis。
func (s *AuthService) Login(ctx context.Context, username, password string) (*userdomain.User, *TokenPair, error) {
	u, err := s.users.GetByUsername(ctx, username)
	if err != nil {
		if errors.Is(err, userdomain.ErrNotFound) {
			return nil, nil, userdomain.ErrInvalidCredentials
		}
		return nil, nil, err
	}
	if err := userdomain.CheckPassword(u.Password, password); err != nil {
		return nil, nil, userdomain.ErrInvalidCredentials
	}

	pair, err := s.issueTokens(ctx, u.ID)
	if err != nil {
		return nil, nil, err
	}
	return u, pair, nil
}

// Refresh 用有效的 refresh token 换新的 access token。
func (s *AuthService) Refresh(ctx context.Context, refreshToken string) (string, error) {
	claims, err := s.tokens.ParseRefreshToken(refreshToken)
	if err != nil {
		return "", userdomain.ErrInvalidToken
	}
	ok, err := s.refresh.Exists(ctx, claims.ID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", userdomain.ErrInvalidToken
	}
	return s.tokens.GenerateAccessToken(claims.UserID)
}

// Logout 吊销 refresh token。
func (s *AuthService) Logout(ctx context.Context, refreshToken string) error {
	claims, err := s.tokens.ParseRefreshToken(refreshToken)
	if err != nil {
		return nil
	}
	return s.refresh.Revoke(ctx, claims.ID)
}

func (s *AuthService) issueTokens(ctx context.Context, userID int64) (*TokenPair, error) {
	access, err := s.tokens.GenerateAccessToken(userID)
	if err != nil {
		return nil, err
	}
	refreshTok, jti, err := s.tokens.GenerateRefreshToken(userID)
	if err != nil {
		return nil, err
	}
	if err := s.refresh.Save(ctx, jti, userID, s.tokens.RefreshTTL()); err != nil {
		return nil, err
	}
	return &TokenPair{AccessToken: access, RefreshToken: refreshTok}, nil
}
