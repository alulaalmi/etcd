package mvcc

import (
	"sync"

	"go.etcd.io/etcd/server/v3/storage/backend"
)

type store struct {
	mu sync.Mutex

	b backend.Backend

	currentRev     int64
	compactMainRev int64
}

func (s *store) compact(rev int64) (<-chan struct{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if rev <= s.compactMainRev {
		return nil, ErrCompacted
	}

	ch := make(chan struct{})
	go s.compactMain(rev, ch)
	return ch, nil
}

func (s *store) compactMain(rev int64, ch chan struct{}) {
	// Update compactMainRev under lock before starting the compaction transaction
	// to ensure that any concurrent watch requests or syncWatchers runs see the
	// updated compaction revision immediately.
	s.mu.Lock()
	s.compactMainRev = rev
	s.mu.Unlock()

	tx := s.b.BatchTx()
	tx.Lock()
	// Perform compaction deletions...
	tx.Unlock()

	close(ch)
}
