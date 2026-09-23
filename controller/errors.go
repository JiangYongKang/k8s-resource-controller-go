package controller

import "errors"

// 写入被拒绝的可区分原因。
var (
	// ErrNotFound 对象不存在（且没有墓碑）。
	ErrNotFound = errors.New("object not found")
	// ErrVersionConflict 携带的版本已过期（乐观并发冲突）。
	ErrVersionConflict = errors.New("version conflict: resourceVersion has expired")
	// ErrTerminating 对象处于删除中，普通写入被拒绝。
	ErrTerminating = errors.New("object is terminating: write rejected")
	// ErrAlreadyExists 创建时对象已存在。
	ErrAlreadyExists = errors.New("object already exists")
)
