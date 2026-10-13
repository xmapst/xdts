package auth

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type MemorySessionStore struct {
	mu     sync.RWMutex
	byHash map[string]Session
	nextID uint64
	now    func() time.Time
}

// NewMemorySessionStore 构造一个内存会话存储。
func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{
		byHash: make(map[string]Session),
		now:    time.Now,
	}
}

// NewMemorySessionStoreWithClock 构造一个内存会话存储，用可控时钟替代
// time.Now——只供测试注入使用（验证过期/续期逻辑不必真的等待）。
func NewMemorySessionStoreWithClock(now func() time.Time) *MemorySessionStore {
	s := NewMemorySessionStore()
	s.now = now
	return s
}

var _ ISessionStore = (*MemorySessionStore)(nil)

func (s *MemorySessionStore) Issue(_ context.Context, userID, username string, role Role, authSource AuthSource, clientIP, userAgent string, ttl time.Duration) (string, Session, error) {
	token, err := newToken()
	if err != nil {
		return "", Session{}, err
	}

	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	sess := Session{
		ID:         fmt.Sprintf("sess-%d", s.nextID),
		TokenHash:  hashToken(token),
		UserID:     userID,
		Username:   username,
		Role:       role,
		AuthSource: authSource,
		CreatedAt:  now,
		ExpiresAt:  now.Add(ttl),
		ClientIP:   clientIP,
		UserAgent:  userAgent,
	}
	s.byHash[sess.TokenHash] = sess
	return token, sess, nil
}

func (s *MemorySessionStore) Verify(_ context.Context, token string) (Session, error) {
	hash := hashToken(token)

	s.mu.RLock()
	sess, ok := s.byHash[hash]
	s.mu.RUnlock()
	if !ok {
		return Session{}, ErrSessionNotFound
	}
	if sess.Expired(s.now()) {
		s.mu.Lock()
		delete(s.byHash, hash)
		s.mu.Unlock()
		return Session{}, ErrSessionNotFound
	}
	return sess, nil
}

func (s *MemorySessionStore) Renew(_ context.Context, token string, ttl time.Duration) (Session, error) {
	hash := hashToken(token)

	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.byHash[hash]
	if !ok {
		return Session{}, ErrSessionNotFound
	}
	now := s.now()
	if sess.Expired(now) {
		delete(s.byHash, hash)
		return Session{}, ErrSessionNotFound
	}
	sess.ExpiresAt = now.Add(ttl)
	s.byHash[hash] = sess
	return sess, nil
}

func (s *MemorySessionStore) Revoke(_ context.Context, token string) error {
	hash := hashToken(token)
	s.mu.Lock()
	delete(s.byHash, hash)
	s.mu.Unlock()
	return nil
}

func (s *MemorySessionStore) RevokeAllForUser(_ context.Context, userID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for hash, sess := range s.byHash {
		if sess.UserID == userID {
			delete(s.byHash, hash)
		}
	}
	return nil
}
