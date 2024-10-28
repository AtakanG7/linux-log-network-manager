// internal/log/collect.go
package log

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

type MessageType string

const (
	TypeLogList MessageType = "log_list"
	TypeLogData MessageType = "log_data"
)

type Message struct {
	Type    MessageType `json:"type"`
	Payload interface{} `json:"payload"`
}

type SearchRequest struct {
	Files    []string `json:"files"`
	Keywords []string `json:"keywords"`
	MaxLines int      `json:"max_lines"`
}

type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Filename  string    `json:"filename"`
	Line      string    `json:"line"`
	LineNum   int       `json:"line_num"`
}

type LogFile struct {
	Path      string    `json:"path"`
	Size      int64     `json:"size"`
	ModTime   time.Time `json:"mod_time"`
	IsGzipped bool      `json:"is_gzipped"`
}

type Collector struct {
	redisClient *redis.Client
	logChan     chan []LogEntry
	searchChan  chan SearchRequest
	knownFiles  map[string]time.Time
	mutex       sync.RWMutex
}

func New(redisAddr string) (*Collector, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   0,
	})

	// Test Redis connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	return &Collector{
		redisClient: rdb,
		logChan:     make(chan []LogEntry, 100),
		searchChan:  make(chan SearchRequest, 10),
		knownFiles:  make(map[string]time.Time),
	}, nil
}

func (c *Collector) Start(ctx context.Context) {
	// Initial updatedb
	if err := c.updateDatabase(); err != nil {
		log.Printf("Initial updatedb failed: %v", err)
	}

	// Start file discovery
	go c.startFileDiscovery(ctx)

	// Start search handler
	go c.handleSearches(ctx)
}

func (c *Collector) updateDatabase() error {
	cmd := exec.Command("sudo", "updatedb")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("updatedb failed: %w", err)
	}
	return nil
}

func (c *Collector) startFileDiscovery(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	// Initial discovery
	if files, err := c.findLogFiles(); err == nil {
		c.storeAndSendFiles(ctx, files)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.updateDatabase(); err != nil {
				log.Printf("Failed to update database: %v", err)
				continue
			}

			files, err := c.findLogFiles()
			if err != nil {
				log.Printf("Failed to find files: %v", err)
				continue
			}

			c.storeAndSendFiles(ctx, files)
		}
	}
}

func (c *Collector) storeAndSendFiles(ctx context.Context, files []LogFile) {
	if len(files) > 0 {
		// Store in Redis
		if data, err := json.Marshal(files); err == nil {
			c.redisClient.Set(ctx, "discovered_logs", data, 24*time.Hour)
		}

		// Update known files
		c.mutex.Lock()
		for _, file := range files {
			c.knownFiles[file.Path] = file.ModTime
		}
		c.mutex.Unlock()

		// Send through channel
		select {
		case c.logChan <- []LogEntry{{
			Timestamp: time.Now(),
			Line:      fmt.Sprintf("Found %d log files", len(files)),
		}}:
		default:
			log.Printf("Channel full, couldn't send file count")
		}
	}
}

func (c *Collector) findLogFiles() ([]LogFile, error) {
	// Use locate command to find log files
	cmd := exec.Command("locate", "--regex", "\\.(log|txt|gz)$")
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

func (c *Collector) handleSearches(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case search := <-c.searchChan:
			go c.processSearch(ctx, search)
		}
	}
}

func (c *Collector) processSearch(ctx context.Context, search SearchRequest) {
	var wg sync.WaitGroup
	for _, file := range search.Files {
		wg.Add(1)
		go func(filepath string) {
			defer wg.Done()
			c.searchFile(ctx, filepath, search.Keywords)
		}(file)
	}
	wg.Wait()
}

func (c *Collector) searchFile(ctx context.Context, filepath string, keywords []string) {
	file, err := os.Open(filepath)
	if err != nil {
		log.Printf("Failed to open file %s: %v", filepath, err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNum := 0
	var batch []LogEntry

	// Use larger buffer for scanning
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
			lineNum++
			line := scanner.Text()

			for _, keyword := range keywords {
				if strings.Contains(line, keyword) {
					batch = append(batch, LogEntry{
						Timestamp: time.Now(),
						Filename:  filepath,
						Line:      line,
						LineNum:   lineNum,
					})
					break
				}
			}

			// Send batch if it's full
			if len(batch) >= 100 {
				select {
				case c.logChan <- batch:
					batch = make([]LogEntry, 0, 100)
				default:
					log.Printf("Channel full, dropping batch for %s", filepath)
				}
			}
		}
	}

	// Send remaining entries
	if len(batch) > 0 {
		select {
		case c.logChan <- batch:
		default:
			log.Printf("Channel full, dropping final batch for %s", filepath)
		}
	}
}

func (c *Collector) GetDiscoveredFiles() []LogFile {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	files := make([]LogFile, 0, len(c.knownFiles))
	for path, modTime := range c.knownFiles {
		if info, err := os.Stat(path); err == nil {
			files = append(files, LogFile{
				Path:      path,
				Size:      info.Size(),
				ModTime:   modTime,
				IsGzipped: strings.HasSuffix(path, ".gz"),
			})
		}
	}
	return files
}

func (c *Collector) GetLogChannel() <-chan []LogEntry {
	return c.logChan
}

func (c *Collector) SubmitSearch(search SearchRequest) {
	select {
	case c.searchChan <- search:
	default:
		log.Printf("Search channel full, dropping search request")
	}
}

func isLogFile(path string) bool {
	path = strings.ToLower(path)
	extensions := []string{".log", ".txt", ".gz"}
	for _, ext := range extensions {
		if strings.HasSuffix(path, ext) {
			return true
		}
	}
	return false
}

func isReadable(path string) bool {
	_, err := os.Open(path)
	return err == nil
}
