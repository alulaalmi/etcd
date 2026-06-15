package mvcc

import (
	"sync"
	"time"

	"go.etcd.io/etcd/server/v3/storage/backend"
)

// watchableStore wraps store and implements watchable interface
type watchableStore struct {
	*store

	mu sync.Mutex

	victims []watcherBatch
	unsynced watcherGroup
	synced watcherGroup

	stopc chan struct{
	}
	wg sync.WaitGroup
}

func (s *watchableStore) watch(key, end []byte, startRev int64, id WatchID, f filterFunc) (*watcher, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.store.mu.Lock()
	defer s.store.mu.Unlock()

	compactRev := s.store.compactMainRev
	if startRev > 0 && startRev < compactRev {
		return nil, ErrCompacted
	}

	wa := &watcher{
		key:    key,
		end:    end,
		id:     id,
		f:      f,
		cur:    startRev,
		minRev: startRev,
	}

	if startRev == 0 || startRev > s.store.currentRev {
		wa.cur = s.store.currentRev + 1
		s.synced.add(wa)
	} else {
		s.unsynced.add(wa)
	}
	return wa, nil
}

func (s *watchableStore) syncWatchers() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.unsynced) == 0 {
		return
	}

	s.store.mu.Lock()
	defer s.store.mu.Unlock()

	// Open the read transaction first to pin the database snapshot
	tx := s.store.b.ReadTx()
	tx.RLock()
	defer tx.RUnlock()

	currRev := s.store.currentRev
	compactionRev := s.store.compactMainRev

	wg := txReadBuffer{}
	for w := range s.unsynced.watchers {
		if w.cur < compactionRev {
			// Watcher requested a revision that has been compacted
			s.unsynced.delete(w)
			w.ch <- WatchResponse{CompactRevision: compactionRev}
			close(w.ch)
			continue
		}

		// Retrieve historical events from the pinned transaction
		revs, keys, err := s.store.rangeKeys(tx, w.key, w.end, w.cur, currRev, 0)
		if err != nil {
			continue
		}

		// If the watcher is caught up, move it to synced
		if len(revs) == 0 {
			s.unsynced.delete(w)
			s.synced.add(w)
			continue
		}

		// Send events to the watcher
		events := make([]Event, len(revs))
		for i := range revs {
			events[i] = Event{
				Type: EventTypePut,
				Kv:   KeyValue{Key: keys[i], ModRevision: revs[i]},
			}
		}
		w.ch <- WatchResponse{Events: events, Revision: currRev}
		w.cur = revs[len(revs)-1] + 1
	}
}
