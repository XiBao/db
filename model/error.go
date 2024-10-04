package model

import "errors"

var (
	ErrEmptyKey = errors.New("empty key")
	ErrNotFound = errors.New("not found")
)
