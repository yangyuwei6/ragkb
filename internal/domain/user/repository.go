package user

import "context"

// UserRepo 是用户持久化契约。
// infra/mysql 提供实现，service 面向接口编排。
type UserRepo interface {
	Create(ctx context.Context, u *User) error
	GetByUsername(ctx context.Context, username string) (*User, error)
	GetByID(ctx context.Context, id int64) (*User, error)
}

// TenantRepo 归属 user 领域，承载“用户与租户成员关系”相关的仓储能力。
// 虽然不再保留独立 tenant 模块，但多租户能力本身仍然需要。
type TenantRepo interface {
	CreateWithMembership(ctx context.Context, tenant *Tenant, isPrimary bool) error
	SetPrimary(ctx context.Context, userID int64, tenantTag string) error
	ListByUser(ctx context.Context, userID int64) ([]Tenant, error)
	GetPrimaryTag(ctx context.Context, userID int64) (string, error)
	IsMember(ctx context.Context, userID int64, tenantTag string) (bool, error)
}
