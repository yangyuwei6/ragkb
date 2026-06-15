package user

import "time"

// 用户角色常量，对齐 users.role 枚举。
const (
	RoleUser  = "USER"
	RoleAdmin = "ADMIN"
)

// User 是用户领域模型，对齐 users 表。
// 贫血模型：只承载数据，业务逻辑放在 service 层。
type User struct {
	ID            int64     `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	Username      string    `gorm:"column:username" json:"username"`
	Password      string    `gorm:"column:password" json:"-"` // bcrypt 哈希，绝不出现在 JSON 里
	Role          string    `gorm:"column:role" json:"role"`
	PrimaryTenant string    `gorm:"-" json:"primaryTenant,omitempty"`
	CreatedAt     time.Time `gorm:"column:created_at" json:"createdAt"`
	UpdatedAt     time.Time `gorm:"column:updated_at" json:"updatedAt"`
}

// TableName 显式指定表名，避免 gorm 复数推断歧义。
func (User) TableName() string { return "users" }

// Tenant 是租户/组织模型，对齐 tenants 表。
// 它放在 user 领域下，表示“用户所在的访问空间”，而不是单独的业务模块。
type Tenant struct {
	ID        int64     `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	Tag       string    `gorm:"column:tag" json:"tag"`
	Name      string    `gorm:"column:name" json:"name"`
	ParentTag *string   `gorm:"column:parent_tag" json:"parentTag,omitempty"`
	CreatedBy int64     `gorm:"column:created_by" json:"createdBy"`
	CreatedAt time.Time `gorm:"column:created_at" json:"createdAt"`
	UpdatedAt time.Time `gorm:"column:updated_at" json:"updatedAt"`
	IsPrimary bool      `gorm:"-" json:"isPrimary"`
}

// TableName 显式指定租户表名。
func (Tenant) TableName() string { return "tenants" }
