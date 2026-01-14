package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	corestore "cosmossdk.io/core/store"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/slab"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/kvstore"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

const memtableMode = "adaptive"

const (
	envDisableWAL          = "TREEDB_BENCH_DISABLE_WAL"
	envDisableBG           = "TREEDB_BENCH_DISABLE_BG"
	envRelaxedSync         = "TREEDB_BENCH_RELAXED_SYNC"
	envDisableValueLog     = "TREEDB_BENCH_DISABLE_VALUE_LOG"
	envDisableReadChecksum = "TREEDB_BENCH_DISABLE_READ_CHECKSUM"
	envAllowUnsafe         = "TREEDB_BENCH_ALLOW_UNSAFE"
	envMode                = "TREEDB_BENCH_MODE"
	envPinSnapshot         = "TREEDB_BENCH_PIN_SNAPSHOT"
	envProfile             = "TREEDB_BENCH_PROFILE"
	envReuseReads          = "TREEDB_BENCH_REUSE_READS"
	envSlabCompression     = "TREEDB_SLAB_COMPRESSION"
	envLeafPrefix          = "TREEDB_LEAF_PREFIX_COMPRESSION"
	envForcePointers       = "TREEDB_FORCE_VALUE_POINTERS"
	envAllowView           = "TREEDB_BENCH_ALLOW_VIEW"
)

// TreeDBAdapter adapts TreeDB to the IAVL v1 db.DB interface.
type TreeDBAdapter struct {
	db         *treedb.DB
	kv         *treedbadapter.DB
	snap       *treedb.Snapshot
	reuseReads bool
	readBuf    []byte
	allowView  bool
}

func (d *TreeDBAdapter) PinSnapshot() {
	if d.snap != nil {
		d.snap.Close()
	}
	d.snap = d.db.AcquireSnapshot()
}

func (d *TreeDBAdapter) UnpinSnapshot() {
	if d.snap != nil {
		d.snap.Close()
		d.snap = nil
	}
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

func envInt64(name string, defaultValue int64) int64 {
	v, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return defaultValue
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return defaultValue
	}
	return n
}

func envString(name string, defaultValue string) string {
	v, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return defaultValue
	}
	return v
}

func setAllowUnsafe(opts *treedb.Options, allow bool) {
	v := reflect.ValueOf(opts).Elem()
	field := v.FieldByName("AllowUnsafe")
	if !field.IsValid() || !field.CanSet() {
		return
	}
	if field.Kind() == reflect.Bool {
		field.SetBool(allow)
	}
}

func applyProfile(opts *treedb.Options, profile string) {
	switch strings.ToLower(strings.TrimSpace(profile)) {
	case "durable":
		treedb.ApplyProfile(opts, treedb.ProfileDurable)
	case "fast":
		treedb.ApplyProfile(opts, treedb.ProfileFast)
	case "bench":
		treedb.ApplyProfile(opts, treedb.ProfileBench)
	case "compressed":
		treedb.ApplyProfile(opts, treedb.ProfileCompressed)
	case "compressed_fast", "fast_compressed":
		treedb.ApplyProfile(opts, treedb.ProfileCompressedFast)
	}
}

func parseCompressionKind(raw string) (slab.CompressionKind, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "zstd", "zstandard":
		return slab.CompressionZSTD, true
	case "none", "off", "disabled":
		return slab.CompressionNone, true
	default:
		return slab.CompressionNone, false
	}
}

func NewTreeDBAdapter(dir string, name string) (*TreeDBAdapter, error) {
	dbPath := filepath.Join(dir, name)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("error creating treedb directory: %w", err)
	}

	disableWAL := envBool(envDisableWAL, false)
	disableBG := envBool(envDisableBG, false)
	pinSnapshot := envBool(envPinSnapshot, false)
	reuseReads := envBool(envReuseReads, false)
	relaxedSync := envBool(envRelaxedSync, true)
	disableValueLog := envBool(envDisableValueLog, false)
	disableReadChecksum := envBool(envDisableReadChecksum, true)
	_, allowUnsafeSet := os.LookupEnv(envAllowUnsafe)
	allowUnsafe := envBool(envAllowUnsafe, false)
	if !allowUnsafeSet && (disableWAL || relaxedSync || disableReadChecksum) {
		allowUnsafe = true
	}

	mode := treedb.ModeCached
	switch strings.ToLower(envString(envMode, "cached")) {
	case "backend", "raw", "uncached":
		mode = treedb.ModeBackend
	}

	openOpts := treedb.Options{
		Dir:          dbPath,
		Mode:         mode,
		MemtableMode: memtableMode,
	}

	applyProfile(&openOpts, envString(envProfile, ""))

	// --- "Unsafe" Performance Options ---
	openOpts.DisableWAL = disableWAL
	openOpts.DisableValueLog = disableValueLog
	openOpts.RelaxedSync = relaxedSync
	openOpts.DisableReadChecksum = disableReadChecksum

	if _, ok := os.LookupEnv(envLeafPrefix); ok {
		openOpts.LeafPrefixCompression = envBool(envLeafPrefix, false)
	}
	if raw, ok := os.LookupEnv(envSlabCompression); ok {
		if kind, parsed := parseCompressionKind(raw); parsed {
			openOpts.SlabCompression.Kind = kind
		}
	}
	if _, ok := os.LookupEnv(envForcePointers); ok {
		openOpts.ForceValuePointers = envBool(envForcePointers, false)
	}

	// --- Tuning for High-Throughput & Large Values ---
	openOpts.FlushThreshold = 64 * 1024 * 1024
	openOpts.FlushBuildConcurrency = 4
	openOpts.ChunkSize = 64 * 1024 * 1024
	openOpts.PreferAppendAlloc = false
	openOpts.KeepRecent = 1
	openOpts.BackgroundIndexVacuumInterval = 15 * time.Second

	// Add Value Log Compaction
	//openOpts.BackgroundCompactionInterval = 1 * time.Second
	//openOpts.BackgroundCompactionDeadRatio = 0.1
	setAllowUnsafe(&openOpts, allowUnsafe)

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

	adapter := &TreeDBAdapter{
		db:         tdb,
		kv:         treedbadapter.Wrap(tdb),
		reuseReads: reuseReads,
		allowView:  envBool(envAllowView, false),
	}
	if pinSnapshot {
		adapter.PinSnapshot()
	}
	return adapter, nil
}

func (d *TreeDBAdapter) Get(key []byte) ([]byte, error) {
	if d.snap != nil {
		val, err := d.snap.GetUnsafe(key)
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				return nil, nil
			}
			return nil, err
		}
		return val, nil
	}
	if d.db == nil {
		return nil, treedb.ErrClosed
	}
	if d.reuseReads {
		val, err := d.db.GetAppend(key, d.readBuf[:0])
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				return nil, nil
			}
			return nil, err
		}
		d.readBuf = val[:0]
		return val, nil
	}
	return d.kv.GetUnsafe(key)
}

func (d *TreeDBAdapter) Has(key []byte) (bool, error) {
	if d.snap != nil {
		return d.snap.Has(key)
	}
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
	d.UnpinSnapshot()
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
			if d.allowView {
				if sv, ok := kb.(interface{ SetView(key, value []byte) error }); ok {
					b.setView = sv.SetView
				}
				if dv, ok := kb.(interface{ DeleteView(key []byte) error }); ok {
					b.deleteView = dv.DeleteView
				}
			}
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
	db         *TreeDBAdapter
	kb         kvstore.Batch
	setView    func(key, value []byte) error
	deleteView func(key []byte) error
	size       int
	done       bool
}

func (b *coreBatch) Set(key, value []byte) error {
	if b.done || b.kb == nil {
		return treedb.ErrClosed
	}
	if value == nil {
		value = []byte{}
	}
	if b.setView != nil {
		if err := b.setView(key, value); err != nil {
			return err
		}
	} else if err := b.kb.Set(key, value); err != nil {
		return err
	}
	b.size += len(key) + len(value)
	return nil
}

func (b *coreBatch) Delete(key []byte) error {
	if b.done || b.kb == nil {
		return treedb.ErrClosed
	}
	if b.deleteView != nil {
		if err := b.deleteView(key); err != nil {
			return err
		}
	} else if err := b.kb.Delete(key); err != nil {
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
