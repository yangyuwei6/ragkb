package service

import (
	"context"
	"errors"

	userdomain "ragkb/internal/domain/user"
)

// UserService 处理当前用户信息查询与资料更新。
type UserService struct {
	users   userdomain.UserRepo
	tenants userdomain.TenantRepo
}

// NewUserService 构造用户服务。
func NewUserService(users userdomain.UserRepo, tenants userdomain.TenantRepo) *UserService {
	return &UserService{users: users, tenants: tenants}
}

// Me 返回当前用户信息。
func (s *UserService) Me(ctx context.Context, userID int64) (*userdomain.User, error) {
	u, err := s.users.GetByID(ctx, userID)
	if err != nil {
		return nil, err
	}
	return u, nil
}

// UpdatePrimaryTenant 设置用户的默认租户。
func (s *UserService) UpdatePrimaryTenant(ctx context.Context, userID int64, tenantTag string) error {
	if err := s.tenants.SetPrimary(ctx, userID, tenantTag); err != nil {
		if errors.Is(err, userdomain.ErrNotFound) {
			return userdomain.ErrForbidden
		}
		return err
	}
	return nil
}

// MyTenants 返回当前用户所属的租户列表。
func (s *UserService) MyTenants(ctx context.Context, userID int64) ([]userdomain.Tenant, error) {
	return s.tenants.ListByUser(ctx, userID)
}
