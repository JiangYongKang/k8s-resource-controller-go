package controller

import (
	"sync"
	"time"
)

// Store 是带乐观并发控制（OCC）的进程内对象存储。
//
// 可见的 resourceVersion 只随“规格/元数据写入”（Create/Update/
// MarkDeleting）递增；控制器内部的状态子资源写入不会推进它，
// 因此后台对账不会让用户手里的版本意外过期（类似 K8s status 子资源）。
// 被拒绝的写入在任何字段被修改之前返回，不改变既有状态。
type Store struct {
	mu      sync.RWMutex
	objects map[string]*Object
	// versions 记录每个对象当前对外可见的 resourceVersion。
	versions map[string]int64
	// tombstones 记录曾被彻底删除的对象名，仅用于日志区分
	// “从未存在”与“曾经存在但已删除”（两者对写入都返回 ErrNotFound）。
	tombstones map[string]int64
	counter    int64
}

// NewStore 创建一个空存储。
func NewStore() *Store {
	return &Store{
		objects:    make(map[string]*Object),
		versions:   make(map[string]int64),
		tombstones: make(map[string]int64),
	}
}

// Create 新建对象，成功时返回对象快照与新的 resourceVersion。
// 对象已存在时返回 ErrAlreadyExists，且不改动任何状态。
func (s *Store) Create(obj *Object) (*Object, int64, error) {
	if obj == nil || obj.Name == "" {
		return nil, 0, ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objects[obj.Name]; ok {
		return nil, 0, ErrAlreadyExists
	}
	now := time.Now()
	obj.Generation = 1
	obj.CreatedAt = now
	obj.UpdatedAt = now
	if obj.Status.Phase == "" {
		obj.Status.Phase = PhasePending
	}
	s.counter++
	rv := s.counter
	s.objects[obj.Name] = obj.Clone()
	s.versions[obj.Name] = rv
	return obj.Clone(), rv, nil
}

// Get 读取对象最新值及其 resourceVersion；不存在返回 ErrNotFound。
func (s *Store) Get(name string) (*Object, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	obj, ok := s.objects[name]
	if !ok {
		return nil, 0, ErrNotFound
	}
	return obj.Clone(), s.versions[name], nil
}

// Mutate 在写锁内对对象做一次条件变更（用户普通更新路径）。
// 拒绝顺序固定为：不存在 -> 版本过期 -> 删除中，保证原因彼此可区分。
// 校验全部通过后才执行 mut；mut 返回错误时同样不保留任何改动
// （在对象的克隆上执行，成功才替换）。
func (s *Store) Mutate(name string, expectedVersion int64, mut func(*Object) error) (*Object, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[name]
	if !ok {
		if lastRV, tomb := s.tombstones[name]; tomb {
			return nil, lastRV, ErrNotFound
		}
		return nil, 0, ErrNotFound
	}
	if s.versions[name] != expectedVersion {
		return nil, s.versions[name], ErrVersionConflict
	}
	if !obj.DeletionTimestamp.IsZero() {
		return nil, s.versions[name], ErrTerminating
	}
	candidate := obj.Clone()
	if mut != nil {
		if err := mut(candidate); err != nil {
			return nil, s.versions[name], err
		}
	}
	candidate.Generation++
	candidate.UpdatedAt = time.Now()
	s.counter++
	rv := s.counter
	s.objects[name] = candidate
	s.versions[name] = rv
	return candidate.Clone(), rv, nil
}

// MarkDeleting 在版本校验通过后给对象打上删除时间戳（终结器语义入口）。
// 对已处于删除中的对象以相同版本重复调用是幂等的：返回当前快照、
// 不产生新版本与新事件。
func (s *Store) MarkDeleting(name string, expectedVersion int64) (*Object, int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[name]
	if !ok {
		return nil, 0, false, ErrNotFound
	}
	if s.versions[name] != expectedVersion {
		return nil, s.versions[name], false, ErrVersionConflict
	}
	if !obj.DeletionTimestamp.IsZero() {
		// 幂等：已经在删除中，不改动任何状态。
		return obj.Clone(), s.versions[name], false, nil
	}
	candidate := obj.Clone()
	candidate.DeletionTimestamp = time.Now()
	candidate.UpdatedAt = candidate.DeletionTimestamp
	s.counter++
	rv := s.counter
	s.objects[name] = candidate
	s.versions[name] = rv
	return candidate.Clone(), rv, true, nil
}

// MutateStatus 是控制器专用的状态子资源写入：不校验版本、不推进
// resourceVersion，因此不会与用户的规格写入互相造成版本失效。
func (s *Store) MutateStatus(name string, mut func(*Object)) (*Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[name]
	if !ok {
		return nil, ErrNotFound
	}
	candidate := obj.Clone()
	mut(candidate)
	s.objects[name] = candidate
	return candidate.Clone(), nil
}

// ClaimSideEffect 原子地把 SideEffectDone 从 false 翻转为 true。
// 返回 transitioned=true 表示调用者赢得了执行副作用的权利；
// 并发对账中只有一个调用者能成功，从根上消除重复副作用。
func (s *Store) ClaimSideEffect(name string) (transitioned bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[name]
	if !ok {
		return false, ErrNotFound
	}
	candidate := obj.Clone()
	if candidate.Status.SideEffectDone {
		return false, nil
	}
	candidate.Status.SideEffectDone = true
	s.objects[name] = candidate
	return true, nil
}

// ClaimFinalize 原子地把 FinalizeDone 从 false 翻转为 true（终结去重）。
func (s *Store) ClaimFinalize(name string) (transitioned bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[name]
	if !ok {
		return false, ErrNotFound
	}
	candidate := obj.Clone()
	if candidate.Status.FinalizeDone {
		return false, nil
	}
	candidate.Status.FinalizeDone = true
	s.objects[name] = candidate
	return true, nil
}

// Remove 彻底移除对象（终结器清空后由控制器调用）。
func (s *Store) Remove(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rv := s.versions[name]
	delete(s.objects, name)
	delete(s.versions, name)
	s.tombstones[name] = rv
	return nil
}

// List 返回全部对象的快照与版本。
func (s *Store) List() map[string]*Object {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*Object, len(s.objects))
	for k, v := range s.objects {
		out[k] = v.Clone()
	}
	return out
}
