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
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	chunkSize            = 10 * 1024 * 1024 // 10MB chunks for processing
	maxBatchSize         = 1000
	deliveryRateLimit    = 1 * time.Second // Time between sending batches to client
	redisKeyExpiry       = 24 * time.Hour  // How long to keep results in Redis
	defaultChannelBuffer = 200             // Default size for the log channel buffer
	metricsInterval      = 2 * time.Second // Interval for collecting metrics
	memoryThreshold      = 80.0            // Maximum memory usage percentage
	cpuThreshold         = 80.0            // Maximum CPU usage percentage
	latencyThreshold     = 1.3             // Maximum latency increase factor
)

type ProcessingMetrics struct {
	ProcessingSpeed float64 // lines per second
	MemoryUsage     float64 // percentage
	CPUUsage        float64 // percentage
	BatchLatency    float64 // milliseconds
	ErrorRate       float64 // percentage
	Timestamp       time.Time
}

type ScalingController struct {
	currentWorkers int
	maxWorkers     int
	metrics        []ProcessingMetrics
	mutex          sync.RWMutex
	threshold      float64
}

type LogFile struct {
	Path        string    `json:"path"`
	ParentPath  string    `json:"parent_path"`
	Name        string    `json:"name"`
	IsDirectory bool      `json:"is_directory"`
	Size        int64     `json:"size"`
	ModTime     time.Time `json:"mod_time"`
	IsGzipped   bool      `json:"is_gzipped"`
}

type NotificationType string

const (
	Warning NotificationType = "warning"
	Error   NotificationType = "error"
	Other   NotificationType = "other"
)

type LogEntry struct {
	Filename  string           `json:"filename"`
	Line      string           `json:"line"`
	LineNum   int              `json:"line_num"`
	Timestamp time.Time        `json:"timestamp"`
	Type      NotificationType `json:"notification_type"`
}

type FileProcessRequest struct {
	Files    []string `json:"files"`
	Keywords []string `json:"keywords"`
}

type CollectorStats struct {
	ProcessedFiles    int64
	ProcessedLines    int64
	BytesProcessed    int64
	BufferedBatches   int64
	DeliveredBatches  int64
	Errors            int64
	LastError         string
	LastErrorTime     time.Time
	CurrentWorkers    int
	ProcessingSpeed   float64
	AverageLatency    float64
	LastMetricsUpdate time.Time
}

type Collector struct {
	redisClient  *redis.Client
	logChan      chan []LogEntry
	knownFiles   map[string]LogFile
	mutex        sync.RWMutex
	lastUpdate   time.Time
	updatePeriod time.Duration
	stats        CollectorStats
	statsMutex   sync.RWMutex
	controller   *ScalingController
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
		logChan:      make(chan []LogEntry, defaultChannelBuffer),
		knownFiles:   make(map[string]LogFile),
		updatePeriod: 1 * time.Minute,
		controller: &ScalingController{
			currentWorkers: 1,
			threshold:      0.7,
		},
	}, nil
}

func (c *Collector) Start(ctx context.Context) {
	if err := c.updateLogFiles(ctx); err != nil {
		log.Printf("Initial log file discovery failed: %v", err)
	}

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

func (c *Collector) ProcessFiles(ctx context.Context, req FileProcessRequest) error {
	searchID := fmt.Sprintf("search:%d", time.Now().UnixNano())

	// Initialize search metadata
	searchMeta := map[string]interface{}{
		"status":     "processing",
		"start_time": time.Now().Format(time.RFC3339),
		"files":      strings.Join(req.Files, ","),
		"keywords":   strings.Join(req.Keywords, ","),
	}

	if err := c.redisClient.HSet(ctx, fmt.Sprintf("%s:meta", searchID), searchMeta).Err(); err != nil {
		return fmt.Errorf("failed to store search metadata: %w", err)
	}

	// Update controller max workers
	c.controller.maxWorkers = len(req.Files)

	// Create work queue
	fileQueue := make(chan string, len(req.Files))
	for _, file := range req.Files {
		fileQueue <- file
	}
	close(fileQueue)

	// Start metrics collection
	metricsDone := make(chan struct{})
	go c.controller.collectMetrics(ctx, c, metricsDone)

	var wg sync.WaitGroup
	errChan := make(chan error, len(req.Files))

	// Initial worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		for filePath := range fileQueue {
			if err := c.processFile(ctx, filePath, req.Keywords, searchID); err != nil {
				errChan <- fmt.Errorf("error processing %s: %w", filePath, err)
				continue
			}

			if c.controller.shouldScaleUp() {
				c.controller.mutex.Lock()
				if c.controller.currentWorkers < c.controller.maxWorkers {
					c.controller.currentWorkers++
					c.updateWorkerCount(c.controller.currentWorkers)
					wg.Add(1)
					go func() {
						defer wg.Done()
						for filePath := range fileQueue {
							if err := c.processFile(ctx, filePath, req.Keywords, searchID); err != nil {
								errChan <- fmt.Errorf("error processing %s: %w", filePath, err)
							}
						}
					}()
				}
				c.controller.mutex.Unlock()
			}
		}
	}()

	wg.Wait()
	close(errChan)
	close(metricsDone)

	// Collect errors
	var errors []string
	for err := range errChan {
		errors = append(errors, err.Error())
		c.recordError(err.Error())
	}

	// Update search metadata
	searchMeta["end_time"] = time.Now().Format(time.RFC3339)
	searchMeta["status"] = "completed"
	if len(errors) > 0 {
		searchMeta["errors"] = strings.Join(errors, "; ")
	}

	if err := c.redisClient.HSet(ctx, fmt.Sprintf("%s:meta", searchID), searchMeta).Err(); err != nil {
		log.Printf("Failed to update search metadata: %v", err)
	}

	// Start the consumer
	go c.startConsumer(ctx, searchID)

	if len(errors) > 0 {
		return fmt.Errorf("processing errors: %s", strings.Join(errors, "; "))
	}

	return nil
}

func (c *Collector) processFile(ctx context.Context, filePath string, keywords []string, searchID string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	var batch []LogEntry
	lineNum := 0
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, chunkSize), chunkSize)

	// Expensive search
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			lineNum++
			line := scanner.Text()

			for _, keyword := range keywords {
				if strings.Contains(line, keyword) {
					batch = append(batch, LogEntry{
						Filename:  filePath,
						Line:      line,
						LineNum:   lineNum,
						Timestamp: time.Now(),
						Type:      getNotificationType(keyword),
					})
					break
				}
			}

			if len(batch) >= maxBatchSize {
				if err := c.bufferBatch(ctx, batch, searchID); err != nil {
					log.Printf("Failed to buffer batch: %v", err)
				}
				batch = make([]LogEntry, 0, maxBatchSize)
			}
		}
	}

	// Buffer remaining entries
	if len(batch) > 0 {
		if err := c.bufferBatch(ctx, batch, searchID); err != nil {
			log.Printf("Failed to buffer final batch: %v", err)
		}
	}

	c.updateStats(lineNum, len(batch))
	return scanner.Err()
}

func getNotificationType(keyword string) NotificationType {
	if keyword == "error" {
		return Error
	} else if keyword == "warning" {
		return Warning
	}
	return Other
}

func (c *Collector) bufferBatch(ctx context.Context, entries []LogEntry, searchID string) error {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if err := json.NewEncoder(gz).Encode(entries); err != nil {
		return fmt.Errorf("failed to encode entries: %w", err)
	}
	gz.Close()

	key := fmt.Sprintf("%s:results", searchID)
	if err := c.redisClient.RPush(ctx, key, buf.Bytes()).Err(); err != nil {
		return fmt.Errorf("failed to store in Redis: %w", err)
	}

	c.redisClient.Expire(ctx, key, redisKeyExpiry)
	c.updateBufferStats(len(entries))
	return nil
}

func (c *Collector) startConsumer(ctx context.Context, searchID string) {
	throttle := time.NewTicker(deliveryRateLimit)
	defer throttle.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-throttle.C:
			entries, err := c.consumeBatch(ctx, searchID)
			if err != nil {
				if err != redis.Nil {
					log.Printf("Error consuming batch: %v", err)
				}
				continue
			}

			if len(entries) > 0 {
				select {
				case c.logChan <- entries:
					c.updateDeliveryStats(len(entries))
				default:
					log.Printf("Channel full, requeueing batch")
					c.bufferBatch(ctx, entries, searchID)
				}
			}
		}
	}
}

func (c *Collector) consumeBatch(ctx context.Context, searchID string) ([]LogEntry, error) {
	key := fmt.Sprintf("%s:results", searchID)
	result, err := c.redisClient.LPop(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	reader := bytes.NewReader([]byte(result))
	gz, err := gzip.NewReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gz.Close()

	var entries []LogEntry
	if err := json.NewDecoder(gz).Decode(&entries); err != nil {
		return nil, fmt.Errorf("failed to decode entries: %w", err)
	}

	return entries, nil
}

func (c *Collector) updateLogFiles(ctx context.Context) error {
	files, err := c.findLogFiles()
	if err != nil {
		return err
	}

	c.mutex.Lock()
	for _, file := range files {
		c.knownFiles[file.Path] = file
	}
	c.lastUpdate = time.Now()
	c.mutex.Unlock()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if err := json.NewEncoder(gz).Encode(files); err != nil {
		return fmt.Errorf("failed to encode files: %w", err)
	}
	gz.Close()

	return c.redisClient.Set(ctx, "log_files", buf.Bytes(), redisKeyExpiry).Err()
}

func (sc *ScalingController) collectMetrics(ctx context.Context, c *Collector, done chan struct{}) {
	ticker := time.NewTicker(metricsInterval)
	defer ticker.Stop()

	var lastStats CollectorStats
	var lastTime time.Time

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			currentStats := c.GetStats()
			currentTime := time.Now()

			if !lastTime.IsZero() {
				timeDiff := currentTime.Sub(lastTime).Seconds()
				metrics := ProcessingMetrics{
					ProcessingSpeed: float64(currentStats.ProcessedLines-lastStats.ProcessedLines) / timeDiff,
					BatchLatency:    float64(currentStats.BufferedBatches-lastStats.BufferedBatches) / float64(currentStats.DeliveredBatches-lastStats.DeliveredBatches),
					ErrorRate:       float64(currentStats.Errors-lastStats.Errors) / float64(currentStats.ProcessedLines-lastStats.ProcessedLines),
					Timestamp:       currentTime,
				}

				metrics.MemoryUsage = getMemoryUsage()
				metrics.CPUUsage = getCPUUsage()

				sc.mutex.Lock()
				sc.metrics = append(sc.metrics, metrics)
				if len(sc.metrics) > 5 {
					sc.metrics = sc.metrics[1:]
				}
				sc.mutex.Unlock()

				c.updateMetrics(metrics)
			}

			lastStats = currentStats
			lastTime = currentTime
		}
	}
}

func (sc *ScalingController) shouldScaleUp() bool {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()

	if len(sc.metrics) < 2 {
		return true
	}

	current := sc.metrics[len(sc.metrics)-1]
	previous := sc.metrics[len(sc.metrics)-2]

	speedImprovement := (current.ProcessingSpeed - previous.ProcessingSpeed) / previous.ProcessingSpeed
	memoryOK := current.MemoryUsage < memoryThreshold
	cpuOK := current.CPUUsage < cpuThreshold
	latencyOK := current.BatchLatency < previous.BatchLatency*latencyThreshold

	return speedImprovement > 0 && memoryOK && cpuOK && latencyOK
}

func (c *Collector) updateStats(lineCount, batchSize int) {
	c.statsMutex.Lock()
	defer c.statsMutex.Unlock()
	c.stats.ProcessedLines += int64(lineCount)
	c.stats.BytesProcessed += int64(batchSize)
	c.stats.ProcessedFiles++
}

func (c *Collector) updateBufferStats(count int) {
	c.statsMutex.Lock()
	defer c.statsMutex.Unlock()
	c.stats.BufferedBatches++
}

func (c *Collector) updateDeliveryStats(count int) {
	c.statsMutex.Lock()
	defer c.statsMutex.Unlock()
	c.stats.DeliveredBatches++
}

func (c *Collector) recordError(errMsg string) {
	c.statsMutex.Lock()
	defer c.statsMutex.Unlock()
	c.stats.Errors++
	c.stats.LastError = errMsg
	c.stats.LastErrorTime = time.Now()
}

func (c *Collector) updateWorkerCount(count int) {
	c.statsMutex.Lock()
	defer c.statsMutex.Unlock()
	c.stats.CurrentWorkers = count
}

func (c *Collector) updateMetrics(metrics ProcessingMetrics) {
	c.statsMutex.Lock()
	defer c.statsMutex.Unlock()
	c.stats.ProcessingSpeed = metrics.ProcessingSpeed
	c.stats.AverageLatency = metrics.BatchLatency
	c.stats.LastMetricsUpdate = time.Now()
}

func (c *Collector) GetStats() CollectorStats {
	c.statsMutex.RLock()
	defer c.statsMutex.RUnlock()
	return c.stats
}

func (c *Collector) GetLogChannel() <-chan []LogEntry {
	return c.logChan
}

func (c *Collector) GetDiscoveredFiles() []LogFile {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	files := make([]LogFile, 0, len(c.knownFiles))
	for _, file := range c.knownFiles {
		if info, err := os.Stat(file.Path); err == nil {
			file.Size = info.Size()
			file.ModTime = info.ModTime()
			files = append(files, file)
		}
	}
	return files
}

// Helper functions for system metrics
func getMemoryUsage() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / float64(m.Sys) * 100
}

func getCPUUsage() float64 {
	// In production, replace with proper CPU monitoring
	// Example: use github.com/shirou/gopsutil
	return 0
}

// Helper functions for file operations
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

func (c *Collector) findLogFiles() ([]LogFile, error) {
	cmd := exec.Command("locate", "-i", "--regex", "\\.log$|\\.log\\.gz$|/log/.*\\.txt$")
	fmt.Print("Locating log files... ")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("locate command failed: %w", err)
	}

	var files []LogFile
	scanner := bufio.NewScanner(bytes.NewReader(output))

	for scanner.Scan() {
		path := scanner.Text()
		if info, err := os.Stat(path); err == nil {
			if isLogFile(path) && isReadable(path) {
				parentPath := filepath.Dir(path)
				if parentPath == path {
					parentPath = ""
				}

				files = append(files, LogFile{
					Path:        path,
					ParentPath:  parentPath,
					Name:        filepath.Base(path),
					IsDirectory: info.IsDir(),
					Size:        info.Size(),
					ModTime:     info.ModTime(),
					IsGzipped:   strings.HasSuffix(path, ".gz"),
				})

				if info.IsDir() && parentPath != "" {
					if parentInfo, err := os.Stat(parentPath); err == nil {
						parentParentPath := filepath.Dir(parentPath)
						if parentParentPath == parentPath {
							parentParentPath = ""
						}

						files = append(files, LogFile{
							Path:        parentPath,
							ParentPath:  parentParentPath,
							Name:        filepath.Base(parentPath),
							IsDirectory: true,
							Size:        parentInfo.Size(),
							ModTime:     parentInfo.ModTime(),
							IsGzipped:   false,
						})
					}
				}
			}
		}
	}

	return files, nil
}
