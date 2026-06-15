package document

import "errors"

// Document 领域错误。
var (
	ErrNotFound        = errors.New("document not found")
	ErrConflict        = errors.New("document conflict")
	ErrForbidden       = errors.New("forbidden")
	ErrBadRequest      = errors.New("bad request")
	ErrFileTooLarge    = errors.New("file too large")
	ErrUnsupportedType = errors.New("unsupported file type")
	ErrPartsIncomplete = errors.New("upload parts incomplete")
)
