package main

import (
	"encoding/binary"
	"errors"
)

var ErrStoreNotInitialized = errors.New("stable store is not initialized")

type StableStorer interface {
	RaftStableStore
	SnapshotsPath() string
}

type StableStore struct {
	db            StableStorer
	snapshotsPath string
}

func NewStableStore(
	snapshotsPath string,
	db StableStorer,
) *StableStore {
	return &StableStore{db: db, snapshotsPath: snapshotsPath}
}

func (cr *StableStore) SnapshotsPath() (path string) {
	if cr == nil || cr.db == nil {
		return "/tmp/raft_snapshots"
	}
	return cr.snapshotsPath
}

// Set is used to set a key/value set outside of the raft log.
func (cr *StableStore) Set(key []byte, val []byte) error {
	if cr == nil || cr.db == nil {
		return ErrStoreNotInitialized
	}
	return cr.db.Set(key, val)
}

// Get is used to retrieve a value from the k/v store by key
func (cr *StableStore) Get(key []byte) ([]byte, error) {
	if cr == nil || cr.db == nil {
		return nil, ErrStoreNotInitialized
	}
	return cr.db.Get(key)

}

// SetUint64 is like Set, but handles uint64 values
func (cr *StableStore) SetUint64(key []byte, val uint64) error {
	if cr == nil || cr.db == nil {
		return ErrStoreNotInitialized
	}
	return cr.db.Set(key, uint64ToBytes(val))
}

func (cr *StableStore) GetUint64(key []byte) (uint64, error) {
	if cr == nil || cr.db == nil {
		return 0, ErrStoreNotInitialized
	}
	val, err := cr.db.Get(key)
	if err != nil {
		return 0, nil // intentionally!
	}

	return bytesToUint64(val), err
}

func bytesToUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// Converts a uint64 to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
