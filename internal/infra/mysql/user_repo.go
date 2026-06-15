package mysql

import (
	"context"
	"errors"

	"gorm.io/gorm"

	userdomain "ragkb/internal/domain/user"
)

type userRepo struct {
	db *gorm.DB
}

type tenantRepo struct {
	db *gorm.DB
}

type userTenant struct {
	UserID    int64  `gorm:"column:user_id"`
	TenantTag string `gorm:"column:tenant_tag"`
	IsPrimary bool   `gorm:"column:is_primary"`
}

func (userTenant) TableName() string { return "user_tenants" }

// NewUserRepo 构造用户仓储（MySQL/GORM 实现）。
func NewUserRepo(db *gorm.DB) userdomain.UserRepo {
	return &userRepo{db: db}
}

// NewTenantRepo 构造“用户-租户成员关系”仓储。
func NewTenantRepo(db *gorm.DB) userdomain.TenantRepo {
	return &tenantRepo{db: db}
}

// Create 创建用户；唯一键冲突翻译为 user.ErrConflict。
func (r *userRepo) Create(ctx context.Context, u *userdomain.User) error {
	if err := r.db.WithContext(ctx).Create(u).Error; err != nil {
		if IsDuplicateKey(err) {
			return userdomain.ErrConflict
		}
		return err
	}
	return nil
}

// GetByUsername 按用户名查询用户。
func (r *userRepo) GetByUsername(ctx context.Context, username string) (*userdomain.User, error) {
	var u userdomain.User
	err := r.db.WithContext(ctx).Where("username = ?", username).First(&u).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, userdomain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := fillPrimaryTenant(ctx, r.db, &u); err != nil {
		return nil, err
	}
	return &u, nil
}

// GetByID 按主键查询用户。
func (r *userRepo) GetByID(ctx context.Context, id int64) (*userdomain.User, error) {
	var u userdomain.User
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&u).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, userdomain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := fillPrimaryTenant(ctx, r.db, &u); err != nil {
		return nil, err
	}
	return &u, nil
}

// CreateWithMembership 创建租户并建立创建者成员关系。
func (r *tenantRepo) CreateWithMembership(ctx context.Context, tenant *userdomain.Tenant, isPrimary bool) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(tenant).Error; err != nil {
			if IsDuplicateKey(err) {
				return userdomain.ErrConflict
			}
			return err
		}

		if isPrimary {
			if err := tx.Model(&userTenant{}).
				Where("user_id = ?", tenant.CreatedBy).
				Update("is_primary", false).Error; err != nil {
				return err
			}
		}

		membership := userTenant{
			UserID:    tenant.CreatedBy,
			TenantTag: tenant.Tag,
			IsPrimary: isPrimary,
		}
		if err := tx.Create(&membership).Error; err != nil {
			if IsDuplicateKey(err) {
				return userdomain.ErrConflict
			}
			return err
		}
		return nil
	})
}

// SetPrimary 设置用户的默认租户。
func (r *tenantRepo) SetPrimary(ctx context.Context, userID int64, tenantTag string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var membership userTenant
		err := tx.Where("user_id = ? AND tenant_tag = ?", userID, tenantTag).First(&membership).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return userdomain.ErrNotFound
		}
		if err != nil {
			return err
		}

		if err := tx.Model(&userTenant{}).
			Where("user_id = ?", userID).
			Update("is_primary", false).Error; err != nil {
			return err
		}
		return tx.Model(&userTenant{}).
			Where("user_id = ? AND tenant_tag = ?", userID, tenantTag).
			Update("is_primary", true).Error
	})
}

// ListByUser 返回当前用户所属租户列表，并带出是否默认租户。
func (r *tenantRepo) ListByUser(ctx context.Context, userID int64) ([]userdomain.Tenant, error) {
	type row struct {
		ID        int64   `gorm:"column:id"`
		Tag       string  `gorm:"column:tag"`
		Name      string  `gorm:"column:name"`
		ParentTag *string `gorm:"column:parent_tag"`
		CreatedBy int64   `gorm:"column:created_by"`
		IsPrimary bool    `gorm:"column:is_primary"`
	}

	var rows []row
	err := r.db.WithContext(ctx).
		Table("tenants").
		Select("tenants.id, tenants.tag, tenants.name, tenants.parent_tag, tenants.created_by, user_tenants.is_primary").
		Joins("JOIN user_tenants ON user_tenants.tenant_tag = tenants.tag").
		Where("user_tenants.user_id = ?", userID).
		Order("user_tenants.is_primary DESC, tenants.created_at ASC").
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}

	tenants := make([]userdomain.Tenant, 0, len(rows))
	for _, item := range rows {
		tenants = append(tenants, userdomain.Tenant{
			ID:        item.ID,
			Tag:       item.Tag,
			Name:      item.Name,
			ParentTag: item.ParentTag,
			CreatedBy: item.CreatedBy,
			IsPrimary: item.IsPrimary,
		})
	}
	return tenants, nil
}

// GetPrimaryTag 返回用户默认租户标识。
func (r *tenantRepo) GetPrimaryTag(ctx context.Context, userID int64) (string, error) {
	var membership userTenant
	err := r.db.WithContext(ctx).
		Where("user_id = ? AND is_primary = ?", userID, true).
		First(&membership).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", userdomain.ErrNotFound
	}
	if err != nil {
		return "", err
	}
	return membership.TenantTag, nil
}

// IsMember 判断用户是否属于某个租户。
func (r *tenantRepo) IsMember(ctx context.Context, userID int64, tenantTag string) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Model(&userTenant{}).
		Where("user_id = ? AND tenant_tag = ?", userID, tenantTag).
		Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func fillPrimaryTenant(ctx context.Context, db *gorm.DB, u *userdomain.User) error {
	var membership userTenant
	err := db.WithContext(ctx).
		Where("user_id = ? AND is_primary = ?", u.ID, true).
		First(&membership).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	u.PrimaryTenant = membership.TenantTag
	return nil
}

var _ userdomain.UserRepo = (*userRepo)(nil)
var _ userdomain.TenantRepo = (*tenantRepo)(nil)
