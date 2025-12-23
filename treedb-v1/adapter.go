package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	corestore "cosmossdk.io/core/store"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/kvstore"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

const memtableMode = "adaptive"

const (
	envDisableWAL = "TREEDB_BENCH_DISABLE_WAL"
	envDisableBG  = "TREEDB_BENCH_DISABLE_BG"
)

// TreeDBAdapter adapts TreeDB to the IAVL v1 db.DB interface.
type TreeDBAdapter struct {
	db *treedb.DB
	kv *treedbadapter.DB
}

type keyArena struct {
	buf []byte
}

func newKeyArena(capacity int) keyArena {
	if capacity <= 0 {
		capacity = 64 * 1024
	}
	return keyArena{buf: make([]byte, 0, capacity)}
}

func (a *keyArena) Copy(key []byte) ([]byte, bool) {
	if len(key) > cap(a.buf)-len(a.buf) {
		return nil, false
	}
	off := len(a.buf)
	a.buf = append(a.buf, key...)
	return a.buf[off : off+len(key)], true
}

func envBool(name string, defaultValue bool) bool {
	v, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}
	v = strings.TrimSpace(strings.ToLower(v))
	if v == "" {
		return true
	}
	switch v {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	}
	if n, err := strconv.Atoi(v); err == nil {
		return n != 0
	}
	return defaultValue
}

func NewTreeDBAdapter(dir string, name string) (*TreeDBAdapter, error) {
	dbPath := filepath.Join(dir, name)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("error creating treedb directory: %w", err)
	}

	disableWAL := envBool(envDisableWAL, false)
	disableBG := envBool(envDisableBG, false)

	openOpts := treedb.Options{
		Dir:          dbPath,
		Mode:         treedb.ModeCached,
		MemtableMode: memtableMode,

		// --- "Unsafe" Performance Options ---
		DisableWAL:          disableWAL,
		RelaxedSync:         true,
		DisableReadChecksum: true,

		// --- Tuning for High-Throughput & Large Values ---
		FlushThreshold:        64 * 1024 * 1024,
		FlushBuildConcurrency: 4,
		ChunkSize:             64 * 1024 * 1024,

		PreferAppendAlloc:             false,
		KeepRecent:                    1,
		BackgroundIndexVacuumInterval: 15 * time.Second,

		// Add Value Log Compaction
		//BackgroundCompactionInterval:  1 * time.Second,
		//BackgroundCompactionDeadRatio: 0.1,
	}

	if disableBG {
		// Background tasks can dominate profile lock/wait time and obscure the
		// hot path; disable them for tighter profiling loops.
		openOpts.BackgroundIndexVacuumInterval = -1
		openOpts.BackgroundCheckpointInterval = -1
		openOpts.MaxWALBytes = -1
		openOpts.BackgroundCheckpointIdleDuration = -1
	}

	tdb, err := treedb.Open(openOpts)
	if err != nil {
		return nil, err
	}

	return &TreeDBAdapter{
		db: tdb,
		kv: treedbadapter.Wrap(tdb),
	}, nil
}

func (d *TreeDBAdapter) Get(key []byte) ([]byte, error) {
	if d.kv == nil {
		return nil, treedb.ErrClosed
	}
	value, err := d.kv.Get(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	// TreeDB may return a read-only view into mmaped pages/slabs; iavl's DB
	// contract treats returned bytes as read-only, so returning the view avoids
	// a high allocation rate from per-Get copying.
	return value, nil
}

func (d *TreeDBAdapter) Has(key []byte) (bool, error) {
	if d.kv == nil {
		return false, treedb.ErrClosed
	}
	return d.kv.Has(key)
}

func (d *TreeDBAdapter) Iterator(start, end []byte) (corestore.Iterator, error) {
	if d.kv == nil {
		return nil, treedb.ErrClosed
	}
	it, err := d.kv.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return &coreIterator{iter: it, start: start, end: end}, nil
}

func (d *TreeDBAdapter) ReverseIterator(start, end []byte) (corestore.Iterator, error) {
	if d.kv == nil {
		return nil, treedb.ErrClosed
	}
	it, err := d.kv.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	return &coreIterator{iter: it, start: start, end: end}, nil
}

func (d *TreeDBAdapter) Close() error {
	if d.db == nil {
		return nil
	}
	err := d.db.Close()
	d.db = nil
	d.kv = nil
	return err
}

func (d *TreeDBAdapter) NewBatch() corestore.Batch {
	return d.NewBatchWithSize(16)
}

func (d *TreeDBAdapter) NewBatchWithSize(size int) corestore.Batch {
	if size <= 0 {
		size = 16
	}
	b := &coreBatch{db: d}
	if d.kv != nil {
		kb, err := d.kv.NewBatch()
		if err == nil {
			b.kb = kb
		}
	}
	return b
}

func (d *TreeDBAdapter) Checkpoint() error {
	if d.kv == nil {
		return treedb.ErrClosed
	}
	return d.kv.Checkpoint()
}

func (d *TreeDBAdapter) Stats() map[string]string {
	if d.kv == nil {
		return nil
	}
	return d.kv.Stats()
}

func (d *TreeDBAdapter) FragmentationReport() (map[string]string, error) {
	if d.db == nil {
		return nil, treedb.ErrClosed
	}
	return d.db.FragmentationReport()
}

type coreBatch struct {
	db   *TreeDBAdapter
	kb   kvstore.Batch
	size int
	done bool
}

func (b *coreBatch) Set(key, value []byte) error {
	if b.done || b.kb == nil {
		return treedb.ErrClosed
	}
	if value == nil {
		value = []byte{}
	}
	if err := b.kb.Set(key, value); err != nil {
		return err
	}
	b.size += len(key) + len(value)
	return nil
}

func (b *coreBatch) Delete(key []byte) error {
	if b.done || b.kb == nil {
		return treedb.ErrClosed
	}
	if err := b.kb.Delete(key); err != nil {
		return err
	}
	b.size += len(key)
	return nil
}

func (b *coreBatch) Write() error {
	if b.done || b.kb == nil {
		return treedb.ErrClosed
	}
	b.done = true
	return b.kb.Commit()
}

func (b *coreBatch) WriteSync() error {
	if b.done || b.kb == nil {
		return treedb.ErrClosed
	}
	b.done = true
	return b.kb.CommitSync()
}

func (b *coreBatch) Close() error {
	if b.kb == nil {
		b.done = true
		return nil
	}
	err := b.kb.Close()
	b.kb = nil
	b.done = true
	return err
}

func (b *coreBatch) GetByteSize() (int, error) {
	return b.size, nil
}

type coreIterator struct {
	iter  kvstore.Iterator
	start []byte
	end   []byte

	keyArena keyArena
	valArena keyArena
}

func (it *coreIterator) Domain() (start, end []byte) { return it.start, it.end }
func (it *coreIterator) Valid() bool                 { return it.iter.Valid() }
func (it *coreIterator) Next()                       { it.iter.Next() }

func (it *coreIterator) Key() []byte {
	if it.keyArena.buf == nil {
		it.keyArena = newKeyArena(64 * 1024)
	}
	key := it.iter.Key()
	out, ok := it.keyArena.Copy(key)
	if ok {
		return out
	}
	out = make([]byte, len(key))
	copy(out, key)
	return out
}

func (it *coreIterator) Value() []byte {
	if it.valArena.buf == nil {
		it.valArena = newKeyArena(256 * 1024)
	}
	val := it.iter.Value()
	out, ok := it.valArena.Copy(val)
	if ok {
		return out
	}
	out = make([]byte, len(val))
	copy(out, val)
	return out
}

func (it *coreIterator) Error() error { return it.iter.Error() }
func (it *coreIterator) Close() error { return it.iter.Close() }

var _ corestore.Batch = (*coreBatch)(nil)
var _ corestore.Iterator = (*coreIterator)(nil)
