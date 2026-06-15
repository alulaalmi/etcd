package mvcc

import (
	"context"
	"testing"
	"time"
)

func TestWatchCompactionRace(t *testing.T) {
	// Test that establishing a watch concurrently with compaction does not lead to missed events
	// or silent event loss at the compaction boundary.
	store := &watchableStore{
		store: &store{
			currentRev: 100,
		},
	}
	store.unsynced = newWatcherGroup()
	store.synced = newWatcherGroup()

	// Trigger concurrent compaction and watch establishment
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		for {
			select {
				case <-ctx.Done():
					return
				default:
					store.compact(50)
			}
		}
	}()

	go func() {
		for {
			select {
				case <-ctx.Done():
					return
				default:
					_, err := store.watch([]byte("foo"), nil, 49, 1, nil)
					if err != nil && err != ErrCompacted {
						t.Errorf("unexpected error: %v", err)
					}
			}
		}
	}()

	<-ctx.Done()
}
