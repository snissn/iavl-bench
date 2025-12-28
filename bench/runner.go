package bench

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"time"

	storev1beta1 "cosmossdk.io/api/cosmos/store/v1beta1"
	"github.com/dustin/go-humanize"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
)

// Tree is a generic interface wrapping a multi-store tree structure.
type Tree interface {
	// Version should return the last committed version. If no version has been committed, it should return 0.
	Version() int64
	// ApplyUpdate should apply a single set or delete to the tree.
	ApplyUpdate(storeKey string, key, value []byte, delete bool) error
	// Commit should persist all changes made since the last commit and return the new version's hash.
	Commit() error
	io.Closer
}

type LoaderParams struct {
	TreeDir     string
	TreeOptions interface{}
	StoreNames  []string
	Logger      *slog.Logger
}

type TreeLoader func(params LoaderParams) (Tree, error)

type RunConfig struct {
	TreeLoader  TreeLoader
	OptionsType interface{}
}

func Run(treeType string, cfg RunConfig) {
	NewRunner(treeType, cfg).Run()
}

type Runner struct {
	*cobra.Command
}

func (r Runner) Run() {
	err := r.Command.Execute()
	if err != nil {
		slog.Error("error running benchmarks", "error", err)
		os.Exit(1)
	}
}

func NewRunner(treeType string, cfg RunConfig) Runner {
	var treeDir string
	var treeOptions string
	var changesetDir string
	var targetVersion int64
	var logHandlerType string
	var logFile string
	var profileDir string
	var profilePrefix string
	var profileAll bool
	var cpuProfile string
	var memProfile string
	var allocsProfile string
	var blockProfile string
	var mutexProfile string
	var traceProfile string
	var goroutineProfile string
	var threadcreateProfile string
	var memProfileRate int
	var blockProfileRate int
	var mutexProfileFraction int
	var pprofHTTP string
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Runs benchmarks for the tree implementation.",
	}
	cmd.Flags().StringVar(&treeDir, "db-dir", "", "Directory for the db's data.")
	cmd.Flags().StringVar(&treeOptions, "db-options", "", "Implementation specific options for the db, in JSON format.")
	cmd.Flags().StringVar(&changesetDir, "changeset-dir", "", "Directory containing the changeset files.")
	cmd.Flags().Int64Var(&targetVersion, "target-version", 0, "Target version to apply changesets up to. If this is empty or 0, all remaining versions in the changeset-dir will be applied.")
	cmd.Flags().StringVar(&logHandlerType, "log-type", "text", "Log handler type. One of 'text' or 'json'.")
	cmd.Flags().StringVar(&logFile, "log-file", "", "If set, log output will be written to this file instead of stdout.")
	cmd.Flags().StringVar(&profileDir, "profile-dir", "", "Write profile outputs into this directory (relative paths are joined to it).")
	cmd.Flags().StringVar(&profilePrefix, "profile-prefix", "", "Optional prefix for default profile filenames when --profile-all is set.")
	cmd.Flags().BoolVar(&profileAll, "profile-all", false, "Enable all common profiles with default filenames (requires --profile-dir or explicit paths).")
	cmd.Flags().StringVar(&cpuProfile, "cpuprofile", "", "Write CPU profile to this file (pprof).")
	cmd.Flags().StringVar(&memProfile, "memprofile", "", "Write heap profile to this file (pprof).")
	cmd.Flags().StringVar(&allocsProfile, "allocsprofile", "", "Write allocs profile to this file (pprof).")
	cmd.Flags().StringVar(&blockProfile, "blockprofile", "", "Write block profile to this file (pprof).")
	cmd.Flags().StringVar(&mutexProfile, "mutexprofile", "", "Write mutex profile to this file (pprof).")
	cmd.Flags().StringVar(&traceProfile, "trace", "", "Write runtime trace to this file.")
	cmd.Flags().StringVar(&goroutineProfile, "goroutineprofile", "", "Write goroutine profile to this file (pprof).")
	cmd.Flags().StringVar(&threadcreateProfile, "threadcreateprofile", "", "Write threadcreate profile to this file (pprof).")
	cmd.Flags().IntVar(&memProfileRate, "memprofile-rate", 0, "If >0, set runtime.MemProfileRate (bytes per sample).")
	cmd.Flags().IntVar(&blockProfileRate, "blockprofile-rate", 0, "If >0, enable block profiling with this sample rate (nanoseconds).")
	cmd.Flags().IntVar(&mutexProfileFraction, "mutexprofile-fraction", 0, "If >0, enable mutex profiling with this fraction (1 records all).")
	cmd.Flags().StringVar(&pprofHTTP, "pprof-http", "", "If set, serve net/http/pprof on this address (e.g. localhost:6060 or :0).")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if treeDir == "" {
			return fmt.Errorf("tree-dir is required")
		}

		if changesetDir == "" {
			return fmt.Errorf("changeset-dir is required")
		}

		changesetInfo, err := readChangesetInfo(changesetDir)
		if err != nil {
			return fmt.Errorf("error reading changeset info file: %w", err)
		}

		if targetVersion <= 0 {
			targetVersion = changesetInfo.Versions
		}

		// decode db options from json
		var opts interface{}
		if cfg.OptionsType != nil {
			opts = reflect.New(reflect.TypeOf(cfg.OptionsType).Elem()).Interface()
			if treeOptions != "" {
				if cfg.OptionsType == nil {
					return fmt.Errorf("db-options provided but no OptionsType set in RunConfig")
				}
				decoder := json.NewDecoder(bytes.NewReader([]byte(treeOptions)))
				// we disallow unknown fields to catch typos with database options
				decoder.DisallowUnknownFields()
				err := decoder.Decode(opts)
				if err != nil {
					return fmt.Errorf("error unmarshaling db-options: %w", err)
				}
			}
		}

		logOut := os.Stdout
		if logFile != "" {
			logOut, err = os.Create(logFile)
			if err != nil {
				return fmt.Errorf("error creating log file: %w", err)
			}
			defer func() {
				err := logOut.Close()
				if err != nil {
					slog.Error("error closing log file", "error", err)
				}
			}()
		}

		var handler slog.Handler
		switch logHandlerType {
		case "text":
			handler = slog.NewTextHandler(logOut, &slog.HandlerOptions{Level: slog.LevelDebug})
		case "json":
			handler = slog.NewJSONHandler(logOut, &slog.HandlerOptions{Level: slog.LevelDebug})
		default:
			return fmt.Errorf("unknown log handler type: %s", logHandlerType)
		}

		// Create a separate handler for tree logger at info level
		var treeHandler slog.Handler
		switch logHandlerType {
		case "text":
			treeHandler = slog.NewTextHandler(logOut, &slog.HandlerOptions{Level: slog.LevelInfo})
		case "json":
			treeHandler = slog.NewJSONHandler(logOut, &slog.HandlerOptions{Level: slog.LevelInfo, AddSource: true})
		default:
			return fmt.Errorf("unknown log handler type: %s", logHandlerType)
		}

		logger := slog.New(handler).With("module", "runner")
		treeLogger := slog.New(treeHandler)

		prof, err := startProfiling(logger, profilingConfig{
			ProfileDir:           profileDir,
			ProfilePrefix:        profilePrefix,
			ProfileAll:           profileAll,
			CPUProfile:           cpuProfile,
			MemProfile:           memProfile,
			AllocsProfile:        allocsProfile,
			BlockProfile:         blockProfile,
			MutexProfile:         mutexProfile,
			TraceProfile:         traceProfile,
			GoroutineProfile:     goroutineProfile,
			ThreadcreateProfile:  threadcreateProfile,
			MemProfileRate:       memProfileRate,
			BlockProfileRate:     blockProfileRate,
			MutexProfileFraction: mutexProfileFraction,
			PprofHTTP:            pprofHTTP,
		})
		if err != nil {
			return err
		}
		if prof != nil {
			defer func() { _ = prof.stop(logger) }()
		}

		logger.Info("Starting benchmark run, loading tree")

		loaderParams := LoaderParams{
			TreeDir:     treeDir,
			TreeOptions: opts,
			StoreNames:  changesetInfo.StoreNames,
			Logger:      treeLogger.With("module", treeType),
		}

		tree, err := cfg.TreeLoader(loaderParams)
		if err != nil {
			return fmt.Errorf("error loading tree: %w", err)
		}

		return run(tree, changesetDir, changesetInfo, runParams{
			TreeType:      treeType,
			TargetVersion: targetVersion,
			Logger:        logger,
			LoaderParams:  loaderParams,
		})
	}

	rootCmd := &cobra.Command{}
	rootCmd.AddCommand(cmd)
	return Runner{Command: rootCmd}
}

type runParams struct {
	TargetVersion int64
	Logger        *slog.Logger
	LoaderParams  LoaderParams
	TreeType      string
}

func run(tree Tree, changesetDir string, changesetInfo changesetInfo, params runParams) error {
	logger := params.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// capture exceptions and log stack trace
	defer func() {
		if r := recover(); r != nil {
			logger.Error("panic occurred", "error", r, "stack", string(debug.Stack()))
		}
	}()

	version := tree.Version()
	target := params.TargetVersion
	logger.Info("starting run",
		"start_version", version,
		"target_version", target,
		"changeset_dir", changesetDir,
		"changeset_info", changesetInfo,
		"db_dir", params.LoaderParams.TreeDir,
		"db_options", params.LoaderParams.TreeOptions,
		"tree_type", params.TreeType,
	)

	captureSystemInfo(logger)

	closeCh := make(chan struct{})
	currentVersion := atomic.Int64{}
	currentVersion.Store(version)
	doneCh := measureBackgroundStats(logger, &currentVersion, params.LoaderParams.TreeDir, closeCh)

	i := 0
	for version < target {
		version++
		currentVersion.Store(version)
		err := applyVersion(logger, tree, changesetDir, version)
		if err != nil {
			return fmt.Errorf("error applying version %d: %w", version, err)
		}
		i++
	}

	err := tree.Close()
	if err != nil {
		return fmt.Errorf("error closing tree: %w", err)
	}
	logger.Info("closed tree")

	logger.Info(
		"benchmark run complete",
		"versions_applied", i,
	)

	close(closeCh)
	<-doneCh

	return nil
}

func captureSystemInfo(logger *slog.Logger) {
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		logger.Warn("could not read build info")
	}

	cpuInfo, err := cpu.Info()
	if err != nil {
		logger.Warn("could not read cpu info", "error", err)
	}

	memInfo, err := mem.VirtualMemory()
	if err != nil {
		logger.Warn("could not read memory info", "error", err)
	}

	hostInfo, err := host.Info()
	if err != nil {
		logger.Warn("could not read host info", "error", err)
	}

	diskInfo, err := disk.Usage("/")
	if err != nil {
		logger.Warn("could not read disk info", "error", err)
	}

	logger.Debug("system info",
		"build_info", buildInfo.String(),
		"cpu_info", cpuInfo,
		"mem_info", memInfo,
		"host_info", hostInfo,
		"disk_info", diskInfo,
	)

	// capture initial disk IO state
	initialDiskCounters, err := disk.IOCounters()
	if err != nil {
		logger.Warn("could not read initial disk io counters", "error", err)
	} else {
		logger.Debug("initial disk io counters", "disk_io_counters", initialDiskCounters)
	}

	// initialize CPU tracking - first call establishes baseline
	initialCPUTimes, err := cpu.Times(true)
	if err != nil {
		logger.Warn("could not read initial cpu times", "error", err)
	} else {
		logger.Debug("initial cpu times", "cpu_times", initialCPUTimes)
	}

	// call cpu.Percent to establish baseline for subsequent calls
	_, _ = cpu.Percent(0, true)
}

func readDelimitedKVPair(reader *bufio.Reader, kv *storev1beta1.StoreKVPair, buf *[]byte) error {
	size, err := binary.ReadUvarint(reader)
	if err != nil {
		return err
	}
	maxInt := int(^uint(0) >> 1)
	if size > uint64(maxInt) {
		return fmt.Errorf("changeset entry too large: %d", size)
	}
	if size == 0 {
		kv.StoreKey = ""
		kv.Delete = false
		kv.Key = kv.Key[:0]
		kv.Value = kv.Value[:0]
		return nil
	}
	b := *buf
	if cap(b) < int(size) {
		b = make([]byte, int(size))
	} else {
		b = b[:int(size)]
	}
	if _, err := io.ReadFull(reader, b); err != nil {
		return err
	}
	kv.StoreKey = ""
	kv.Delete = false
	kv.Key = kv.Key[:0]
	kv.Value = kv.Value[:0]
	if err := (proto.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(b, kv); err != nil {
		return err
	}
	*buf = b
	return nil
}
func applyVersion(logger *slog.Logger, tree Tree, changesetDir string, version int64) error {
	dataFilename := changesetDataFilename(changesetDir, version)
	dataFile, err := os.Open(dataFilename)
	if err != nil {
		return fmt.Errorf("error opening changeset file for version %d: %w", version, err)
	}
	defer func() {
		err := dataFile.Close()
		if err != nil {
			panic(err)
		}
	}()
	reader := bufio.NewReader(dataFile)

	logger.Info("applying changeset", "version", version, "file", dataFilename)
	i := 0
	startTime := time.Now()
	var buf []byte
	for {
		if i%10_000 == 0 && i > 0 {
			logger.Debug("applied changes", "version", version, "count", i)
		}
		storeKVPair := storev1beta1.StoreKVPair{}
		err := readDelimitedKVPair(reader, &storeKVPair, &buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error at entry %d reading changeset: %w", i, err)
		}

		err = tree.ApplyUpdate(storeKVPair.StoreKey, storeKVPair.Key, storeKVPair.Value, storeKVPair.Delete)
		if err != nil {
			return fmt.Errorf("error at entry %d applying update: %w", i, err)
		}

		i++
	}
	logger.Info("applied all changes, commiting", "version", version, "count", i)

	err = tree.Commit()
	if err != nil {
		return fmt.Errorf("error committing version %d: %w", version, err)
	}

	if tree.Version() != version {
		return fmt.Errorf("committed version %d does not match expected version %d", tree.Version(), version)
	}

	duration := time.Since(startTime)
	opsPerSec := float64(i) / duration.Seconds()

	// get mem stats

	logger.Info(
		"committed version",
		"version", version,
		"duration", duration,
		"count", i,
		"ops_per_sec", opsPerSec,
	)

	return nil
}

func measureBackgroundStats(logger *slog.Logger, currentVersion *atomic.Int64, path string, closeCh <-chan struct{}) <-chan struct{} {
	doneChan := make(chan struct{})
	go func() {
		fastStatTicker := time.NewTicker(1 * time.Second)
		diskStatTicker := time.NewTicker(10 * time.Second)
		defer fastStatTicker.Stop()
		defer diskStatTicker.Stop()
		for {
			select {
			case <-fastStatTicker.C:
				// capture mem stats
				var memStats runtime.MemStats
				runtime.ReadMemStats(&memStats)
				logger.Info("mem stats", "version", currentVersion.Load(),
					"alloc", humanize.Bytes(memStats.Alloc),
					"total_alloc", humanize.Bytes(memStats.TotalAlloc),
					"sys", humanize.Bytes(memStats.Sys),
					"num_gc", memStats.NumGC,
					"gc_sys", humanize.Bytes(memStats.GCSys),
					"heap_sys", humanize.Bytes(memStats.HeapSys),
					"heap_idle", humanize.Bytes(memStats.HeapIdle),
					"heap_inuse", humanize.Bytes(memStats.HeapInuse),
					"heap_released", humanize.Bytes(memStats.HeapReleased),
					"heap_objects", memStats.HeapObjects,
					"gc_pause_total", memStats.PauseTotalNs,
					"gc_cpu_fraction", memStats.GCCPUFraction,
				)

				// get cpu utilization data
				cpuPercents, err := cpu.Percent(0, true)
				if err != nil {
					logger.Warn("could not read cpu percent", "error", err)
				}

				cpuTimes, err := cpu.Times(true)
				if err != nil {
					logger.Warn("could not read cpu times", "error", err)
				}
				logger.Info("cpu usage", "version", currentVersion.Load(), "cpu_percents", cpuPercents, "cpu_times", cpuTimes)

				// get disk io stats
				diskIOCounters, err := disk.IOCounters()
				if err != nil {
					logger.Warn("could not read disk io counters", "error", err)
				}
				logger.Info("disk io counters", "version", currentVersion.Load(), "disk_io_counters", diskIOCounters)

			case <-diskStatTicker.C:
				// capture disk usage (expensive operation)
				size := getDirSize(logger, path)
				logger.Info("disk usage", "version", currentVersion.Load(), "size", humanize.Bytes(size))

			case <-closeCh:
				close(doneChan)
				return
			}
		}
	}()
	return doneChan
}

func getDirSize(logger *slog.Logger, path string) uint64 {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			// we don't want to fail the whole operation if there's an error walking a path
			// just log it and continue, the files may change in the meantime
			logger.Warn("error walking path", "path", path, "error", err)
			return nil
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	if err != nil {
		logger.Warn("error getting dir size", "path", path, "error", err)
	}
	return uint64(size)
}
