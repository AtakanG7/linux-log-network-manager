// internal/log/collect.go
package log

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	chunkSize     = 10 * 1024 * 1024 // 10MB chunks for processing
	batchSize     = 1000             // Number of lines to collect before sending
	maxGoroutines = 4                // Maximum number of concurrent file processors
)

type LogFile struct {
	Path      string    `json:"path"`
	Size      int64     `json:"size"`
	ModTime   time.Time `json:"mod_time"`
	IsGzipped bool      `json:"is_gzipped"`
}

type LogEntry struct {
	Filename  string    `json:"filename"`
	Line      string    `json:"line"`
	LineNum   int       `json:"line_num"`
	Timestamp time.Time `json:"timestamp"`
}

type FileProcessRequest struct {
	Files    []string `json:"files"`
	Keywords []string `json:"keywords"`
}

type Collector struct {
	redisClient  *redis.Client
	logChan      chan []LogEntry
	knownFiles   map[string]LogFile // Changed to store LogFile structs
	mutex        sync.RWMutex
	lastUpdate   time.Time
	updatePeriod time.Duration
}

func New(redisAddr string) (*Collector, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	return &Collector{
		redisClient:  rdb,
		logChan:      make(chan []LogEntry, 100),
		knownFiles:   make(map[string]LogFile),
		updatePeriod: 5 * time.Minute,
	}, nil
}

func (c *Collector) Start(ctx context.Context) {
	// Initial discovery
	if err := c.updateLogFiles(ctx); err != nil {
		log.Printf("Initial log file discovery failed: %v", err)
	}

	// Periodic discovery
	ticker := time.NewTicker(c.updatePeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.updateLogFiles(ctx); err != nil {
				log.Printf("Log file discovery failed: %v", err)
			}
		}
	}
}

func (c *Collector) GetDiscoveredFiles() []LogFile {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	files := make([]LogFile, 0, len(c.knownFiles))
	for _, file := range c.knownFiles {
		// Verify file still exists and is accessible
		if info, err := os.Stat(file.Path); err == nil {
			// Update size and modtime if changed
			file.Size = info.Size()
			file.ModTime = info.ModTime()
			files = append(files, file)
		}
	}
	return files
}

func (c *Collector) updateLogFiles(ctx context.Context) error {
	files, err := c.findLogFiles()
	if err != nil {
		return err
	}

	// Update known files
	c.mutex.Lock()
	for _, file := range files {
		c.knownFiles[file.Path] = file
	}
	c.lastUpdate = time.Now()
	c.mutex.Unlock()

	// Compress and store the file list
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if err := json.NewEncoder(gz).Encode(files); err != nil {
		return fmt.Errorf("failed to encode files: %w", err)
	}
	gz.Close()

	// Store in Redis with 1 hour expiry
	return c.redisClient.Set(ctx, "log_files", buf.Bytes(), time.Hour).Err()
}

func (c *Collector) findLogFiles() ([]LogFile, error) {
	cmd := exec.Command("locate", "-i", "--regex", "\\.(log|txt)$|/log/|/logs/")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("locate command failed: %w", err)
	}

	var files []LogFile
	scanner := bufio.NewScanner(bytes.NewReader(output))

	for scanner.Scan() {
		path := scanner.Text()
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			if isLogFile(path) && isReadable(path) {
				files = append(files, LogFile{
					Path:      path,
					Size:      info.Size(),
					ModTime:   info.ModTime(),
					IsGzipped: strings.HasSuffix(path, ".gz"),
				})
			}
		}
	}

	return files, nil
}

func (c *Collector) ProcessFiles(ctx context.Context, req FileProcessRequest) error {
	semaphore := make(chan struct{}, maxGoroutines)
	errChan := make(chan error, len(req.Files))
	var wg sync.WaitGroup

	for _, filePath := range req.Files {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			if err := c.processFile(ctx, path, req.Keywords); err != nil {
				errChan <- fmt.Errorf("error processing %s: %w", path, err)
			}
		}(filePath)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	close(errChan)

	// Collect any errors
	var errors []string
	for err := range errChan {
		errors = append(errors, err.Error())
	}

	if len(errors) > 0 {
		return fmt.Errorf("processing errors: %s", strings.Join(errors, "; "))
	}

	return nil
}

func (c *Collector) processFile(ctx context.Context, filePath string, keywords []string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	var batch []LogEntry
	lineNum := 0
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, chunkSize), chunkSize)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			lineNum++
			line := scanner.Text()

			// Check if line contains any of the keywords
			for _, keyword := range keywords {
				if strings.Contains(line, keyword) {
					batch = append(batch, LogEntry{
						Filename:  filePath,
						Line:      line,
						LineNum:   lineNum,
						Timestamp: time.Now(),
					})
					break
				}
			}

			// Send batch if it's full
			if len(batch) >= batchSize {
				if err := c.sendBatch(ctx, batch); err != nil {
					return fmt.Errorf("failed to send batch: %w", err)
				}
				batch = make([]LogEntry, 0, batchSize)
			}
		}
	}

	// Send remaining entries
	if len(batch) > 0 {
		if err := c.sendBatch(ctx, batch); err != nil {
			return fmt.Errorf("failed to send final batch: %w", err)
		}
	}

	return scanner.Err()
}

func (c *Collector) sendBatch(ctx context.Context, entries []LogEntry) error {
	// Compress the batch
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if err := json.NewEncoder(gz).Encode(entries); err != nil {
		return fmt.Errorf("failed to encode entries: %w", err)
	}
	gz.Close()

	// Send through channel
	select {
	case c.logChan <- entries:
		return nil
	default:
		log.Printf("Warning: Channel full, dropping batch of %d entries", len(entries))
		return nil
	}
}

func (c *Collector) GetLogChannel() <-chan []LogEntry {
	return c.logChan
}

func isLogFile(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	return ext == ".log" || ext == ".txt" || strings.Contains(path, "/log/") || strings.Contains(path, "/logs/")
}

func isReadable(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	file.Close()
	return true
}
