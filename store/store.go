package store

import (
	"sync"

	"github.com/arsenalzp/whyreconcile/causes"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type Store interface {
	Add(req reconcile.Request, cause causes.Cause) bool
	Take(req reconcile.Request) []causes.Cause
}

type CauseStore struct {
	mu     sync.Mutex
	causes map[reconcile.Request][]causes.Cause
}

func (s *CauseStore) Add(req reconcile.Request, cause causes.Cause) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.causes[req] = append(s.causes[req], cause)

	return true
}

func (s *CauseStore) Take(req reconcile.Request) []causes.Cause {
	s.mu.Lock()
	defer s.mu.Unlock()

	causes := s.causes[req]
	delete(s.causes, req)

	return causes
}

func NewCauseStore() *CauseStore {
	return &CauseStore{
		causes: make(map[reconcile.Request][]causes.Cause),
	}
}
