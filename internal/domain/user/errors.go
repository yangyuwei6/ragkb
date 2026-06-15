package user

import "errors"

// User 领域错误。
var (
	ErrNotFound           = errors.New("user not found")
	ErrConflict           = errors.New("user conflict")
	ErrAlreadyExists      = errors.New("user already exists")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrForbidden          = errors.New("forbidden")
	ErrInvalidToken       = errors.New("invalid token")
)
