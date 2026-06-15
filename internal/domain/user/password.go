package user

import "golang.org/x/crypto/bcrypt"

// HashPassword 用 bcrypt 加密明文密码，返回可入库的哈希。
// 使用默认 cost，兼顾安全与登录延迟。
func HashPassword(plain string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// CheckPassword 校验明文与哈希是否匹配。匹配返回 nil，否则返回 error。
func CheckPassword(hash, plain string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(plain))
}
