package monitor

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Cfg the configuration for monitor
type Cfg struct {
	ID               string
	Discovery        string
	Target           string
	Backups          []string
	MonitorInterval  time.Duration
	BatchFetch       int
	RefreshInterval  time.Duration
	LimitTraffic     int64
	TimeoutRead      time.Duration
	TimeoutWrite     time.Duration
	Chunk            int64
	TimeoutConnect   time.Duration
	DiableRetry      bool
	RetriesInterval  time.Duration
	RetriesPerServer int
}

// LastFileName returns file name that store the process info
func (c *Cfg) LastFileName() string {
	return fmt.Sprintf("%s/.last", c.Target)
}

func (c *Cfg) getFiles() ([]string, error) {
	var files []string

	// Walk the file tree rooted at c.Target, skip subdirectories who's depth is larger than 1.
	err := filepath.Walk(c.Target, func(path string, f os.FileInfo, err error) error {
		if f.IsDir() {
			if path == c.Target {
				return nil
			}
			// walk c.Target
			dir, _ := filepath.Split(path)
			if dir != c.Target {
				return filepath.SkipDir
			}
			return nil
		}

		if path == c.LastFileName() {
			return nil
		}
		if len(files) < c.BatchFetch {
			files = append(files, path)
		}
		return nil
	})

	return files, err
}
