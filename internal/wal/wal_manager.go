package wal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type WALManager struct {
	numShards   int
	logFiles    []*os.File
	fileSizes   []int64
	mu          sync.Mutex
	baseDir     string
	maxFileSize int64
	maxFiles    int
}

const (
	defaultMaxFileSize = 100 * 1024 * 1024 // 100MB
	defaultMaxFiles    = 5                 // Keep last 5 WAL files
)

func NewWALManager(numShards int, baseDir string) (*WALManager, error) {
	logFiles := make([]*os.File, numShards)
	fileSizes := make([]int64, numShards)
	for i := 0; i < numShards; i++ {
		shardDir := filepath.Join(baseDir, fmt.Sprintf("shard%d", i))
		if err := os.MkdirAll(shardDir, 0755); err != nil {
			return nil, fmt.Errorf("create shard directory %s: %w", shardDir, err)
		}
		path := filepath.Join(shardDir, "wal.log")
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("open WAL file for shard %d: %w", i, err)
		}
		logFiles[i] = f
		info, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("stat WAL file for shard %d: %w", i, err)
		}
		fileSizes[i] = info.Size()
	}
	return &WALManager{
		numShards:   numShards,
		logFiles:    logFiles,
		fileSizes:   fileSizes,
		baseDir:     baseDir,
		maxFileSize: defaultMaxFileSize,
		maxFiles:    defaultMaxFiles,
	}, nil
}

func (w *WALManager) rotateFile(shardID int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	f := w.logFiles[shardID]
	if err := f.Close(); err != nil {
		return fmt.Errorf("close WAL file for shard %d: %w", shardID, err)
	}

	shardDir := filepath.Join(w.baseDir, fmt.Sprintf("shard%d", shardID))
	currentPath := filepath.Join(shardDir, "wal.log")
	timestamp := time.Now().Format("20060102T150405")
	newPath := filepath.Join(shardDir, fmt.Sprintf("wal-%s.log", timestamp))

	if err := os.Rename(currentPath, newPath); err != nil {
		return fmt.Errorf("rename WAL file for shard %d: %w", shardID, err)
	}

	// Open new WAL file
	f, err := os.OpenFile(currentPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open new WAL file for shard %d: %w", shardID, err)
	}
	w.logFiles[shardID] = f
	w.fileSizes[shardID] = 0

	return nil
}

// Cleanup removes WAL files older than the specified duration.
func (w *WALManager) Cleanup(retention time.Duration) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	cutoff := time.Now().Add(-retention)

	for i := 0; i < w.numShards; i++ {
		shardDir := filepath.Join(w.baseDir, fmt.Sprintf("shard%d", i))
		files, err := filepath.Glob(filepath.Join(shardDir, "wal-*.log"))
		if err != nil {
			return fmt.Errorf("list WAL files for shard %d: %w", i, err)
		}

		for _, file := range files {
			filename := filepath.Base(file)
			// expected format: wal-20060102T150405.log
			if len(filename) < 19 {
				continue
			}
			timeStr := filename[4 : len(filename)-4] // strip "wal-" and ".log"
			t, err := time.Parse("20060102T150405", timeStr)
			if err != nil {
				continue // skip malformed files
			}

			if t.Before(cutoff) {
				if err := os.Remove(file); err != nil {
					return fmt.Errorf("remove old WAL file %s: %w", file, err)
				}
			}
		}
	}
	return nil
}

func (w *WALManager) WriteWAL(shardID int, data []byte) error {
	if shardID >= w.numShards {
		return fmt.Errorf("invalid shard ID %d", shardID)
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fileSizes[shardID] >= w.maxFileSize {
		if err := w.rotateFile(shardID); err != nil {
			return fmt.Errorf("rotate WAL file for shard %d: %w", shardID, err)
		}
	}

	f := w.logFiles[shardID]
	n, err := f.Write(append(data, '\n'))
	if err != nil {
		return fmt.Errorf("write WAL for shard %d: %w", shardID, err)
	}
	w.fileSizes[shardID] += int64(n)
	return nil
}

func (w *WALManager) ReadWAL(shardID int) ([][]byte, error) {
	if shardID >= w.numShards {
		return nil, fmt.Errorf("invalid shard ID %d", shardID)
	}
	data, err := os.ReadFile(w.logFiles[shardID].Name())
	if err != nil {
		return nil, fmt.Errorf("read WAL for shard %d: %w", shardID, err)
	}
	if len(data) == 0 {
		return nil, nil
	}
	lines := bytes.Split(data, []byte{'\n'})
	var entries [][]byte
	for _, line := range lines {
		if len(line) > 0 {
			entries = append(entries, line)
		}
	}
	return entries, nil
}

func (w *WALManager) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i, f := range w.logFiles {
		if err := f.Close(); err != nil {
			return fmt.Errorf("close WAL file for shard %d: %w", i, err)
		}
	}
	return nil
}
