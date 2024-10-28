// internal/agent/agent.go
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	logcollect "diagnostic-agent/internal/log" // aliased to avoid conflict
	"diagnostic-agent/internal/network"
	"diagnostic-agent/internal/tunnel"
)

type MessageType string

const (
	TypeMetrics   MessageType = "metrics"
	TypeLogList   MessageType = "log_list"
	TypeLogData   MessageType = "log_data"
	TypeLogSearch MessageType = "log_search"
	TypeCommand   MessageType = "command"
)

type Message struct {
	Type    MessageType `json:"type"`
	Payload interface{} `json:"payload"`
}

type Agent struct {
	networkCollector *network.Collector
	logCollector     *logcollect.Collector
	tunnel           *tunnel.Tunnel
	redisAddr        string
}

func New(hostAddr, redisAddr string) (*Agent, error) {
	// Initialize network metrics collector
	networkCollector, err := network.New(redisAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create network collector: %w", err)
	}

	// Initialize log collector
	logCollector, err := logcollect.New(redisAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create log collector: %w", err)
	}

	return &Agent{
		networkCollector: networkCollector,
		logCollector:     logCollector,
		tunnel:           tunnel.New(hostAddr),
		redisAddr:        redisAddr,
	}, nil
}

func (a *Agent) Run(ctx context.Context) error {
	log.Println("Starting agent...")

	// Connect tunnel
	if err := a.tunnel.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect tunnel: %w", err)
	}
	defer a.tunnel.Close()

	// Start collectors
	log.Println("Starting collectors...")
	go a.networkCollector.Start(ctx)
	go a.logCollector.Start(ctx)

	// Create a WaitGroup for our goroutines
	var wg sync.WaitGroup
	wg.Add(3) // Commands, Network Metrics, Logs

	// Handle incoming commands
	go func() {
		defer wg.Done()
		a.handleCommands(ctx)
	}()

	// Handle network metrics
	go func() {
		defer wg.Done()
		a.handleNetworkMetrics(ctx)
	}()

	// Handle logs
	go func() {
		defer wg.Done()
		a.handleLogs(ctx)
	}()

	// Wait for all goroutines to finish
	wg.Wait()
	return nil
}

func (a *Agent) handleCommands(ctx context.Context) {
	log.Println("[AGENT] Starting command handler")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			decoder := a.tunnel.GetDecoder()
			if decoder == nil {
				log.Println("[AGENT] No decoder available, waiting...")
				time.Sleep(time.Second)
				continue
			}

			// Set a reasonable timeout for command reading
			if conn := a.tunnel.GetConnection(); conn != nil {
				conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			}

			var msg Message
			if err := decoder.Decode(&msg); err != nil {
				if err == io.EOF || strings.Contains(err.Error(), "i/o timeout") {
					// These are expected, no need to log them
					continue
				}
				log.Printf("[AGENT][ERROR] Failed to decode command: %v", err)
				time.Sleep(time.Second)
				continue
			}

			log.Printf("[AGENT] Received command: %s", msg.Type)
			// Process command...
		}
	}
}

func (a *Agent) handleNetworkMetrics(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case metrics := <-a.networkCollector.Metrics():
			msg := Message{
				Type:    TypeMetrics,
				Payload: metrics,
			}
			if err := a.tunnel.Send(msg); err != nil {
				log.Printf("Failed to send network metrics: %v", err)
			}
		}
	}
}

func (a *Agent) handleLogs(ctx context.Context) {
	log.Println("[AGENT] Starting log handler")
	logChan := a.logCollector.GetLogChannel()
	discoverTicker := time.NewTicker(30 * time.Second)
	defer discoverTicker.Stop()

	// Send initial file list
	if files := a.logCollector.GetDiscoveredFiles(); len(files) > 0 {
		log.Printf("[AGENT] Sending initial file list: found %d files", len(files))
		msg := Message{
			Type:    TypeLogList,
			Payload: files,
		}
		if err := a.tunnel.Send(msg); err != nil {
			log.Printf("[AGENT][ERROR] Failed to send initial file list: %v", err)
		}
	}

	var stats struct {
		messagesSent   int
		bytesProcessed int64
		errorCount     int
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("[AGENT] Log handler shutting down. Stats: messages=%d, bytes=%d, errors=%d",
				stats.messagesSent, stats.bytesProcessed, stats.errorCount)
			return

		case logEntries := <-logChan:
			stats.messagesSent++
			entryCount := len(logEntries)
			log.Printf("[AGENT] Received %d log entries to send", entryCount)

			msg := Message{
				Type:    TypeLogData,
				Payload: logEntries,
			}

			// Calculate approximate size
			if data, err := json.Marshal(msg); err == nil {
				stats.bytesProcessed += int64(len(data))
			}

			if err := a.tunnel.Send(msg); err != nil {
				stats.errorCount++
				log.Printf("[AGENT][ERROR] Failed to send log entries: %v", err)
			} else {
				log.Printf("[AGENT] Successfully sent %d log entries", entryCount)
			}

		case <-discoverTicker.C:
			files := a.logCollector.GetDiscoveredFiles()
			log.Printf("[AGENT] File list update: found %d files", len(files))

			msg := Message{
				Type:    TypeLogList,
				Payload: files,
			}
			if err := a.tunnel.Send(msg); err != nil {
				stats.errorCount++
				log.Printf("[AGENT][ERROR] Failed to send file list update: %v", err)
			}

			// Log periodic stats
			log.Printf("[AGENT] Log handler stats: messages=%d, bytes=%d, errors=%d",
				stats.messagesSent, stats.bytesProcessed, stats.errorCount)
		}
	}
}
