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

	logcollect "diagnostic-agent/internal/log"
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

type LogSearchCommand struct {
	Files    []string `json:"files"`
	Keywords []string `json:"keywords"`
}

type Agent struct {
	networkCollector *network.Collector
	logCollector     *logcollect.Collector
	tunnel           *tunnel.Tunnel
	redisAddr        string
	activeSearches   sync.WaitGroup
}

func New(hostAddr, redisAddr string) (*Agent, error) {
	networkCollector, err := network.New(redisAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create network collector: %w", err)
	}

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

	if err := a.tunnel.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect tunnel: %w", err)
	}
	defer a.tunnel.Close()

	log.Println("Starting collectors...")
	go a.logCollector.Start(ctx)

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		a.handleCommands(ctx)
	}()

	go func() {
		defer wg.Done()
		a.handleNetworkMetrics(ctx)
	}()

	go func() {
		defer wg.Done()
		a.handleLogs(ctx)
	}()

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

			// Set read deadline
			if conn := a.tunnel.GetConnection(); conn != nil {
				conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			}

			// Read and parse the message
			var msg Message
			if err := decoder.Decode(&msg); err != nil {
				if err == io.EOF || strings.Contains(err.Error(), "i/o timeout") {
					continue
				}
				log.Printf("[AGENT][ERROR] Failed to decode message: %v", err)

				// Try to read the raw data for debugging
				rawData := make([]byte, 1024)
				if n, _ := decoder.Buffered().Read(rawData); n > 0 {
					log.Printf("[AGENT][DEBUG] Raw data received: %s", string(rawData[:n]))
				}
				continue
			}

			log.Printf("[AGENT] Received command type: %s", msg.Type)

			// Handle the message based on type
			switch msg.Type {
			case TypeLogSearch:
				// Parse the search command
				var searchCmd LogSearchCommand
				if data, err := json.Marshal(msg.Payload); err == nil {
					if err := json.Unmarshal(data, &searchCmd); err == nil {
						log.Printf("[AGENT] Processing search command for %d files: %v",
							len(searchCmd.Files), searchCmd.Files)
						log.Printf("[AGENT] Search keywords: %v", searchCmd.Keywords)

						a.handleSearchCommand(ctx, searchCmd)
					} else {
						log.Printf("[AGENT][ERROR] Failed to parse search command: %v", err)
					}
				} else {
					log.Printf("[AGENT][ERROR] Failed to prepare search command: %v", err)
				}

			default:
				log.Printf("[AGENT] Received unhandled message type: %s", msg.Type)
			}
		}
	}
}

func (a *Agent) handleSearchCommand(ctx context.Context, cmd LogSearchCommand) {
	log.Printf("[AGENT] Processing search command for %d files with %d keywords",
		len(cmd.Files), len(cmd.Keywords))

	a.activeSearches.Add(1)
	go func() {
		defer a.activeSearches.Done()

		req := logcollect.FileProcessRequest{
			Files:    cmd.Files,
			Keywords: cmd.Keywords,
		}

		if err := a.logCollector.ProcessFiles(ctx, req); err != nil {
			log.Printf("[AGENT][ERROR] File processing failed: %v", err)
		}
	}()
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
			// Wait for any active searches to complete
			a.activeSearches.Wait()
			return

		case logEntries := <-logChan:
			if len(logEntries) == 0 {
				continue
			}

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

			log.Printf("[AGENT] Log handler stats: messages=%d, bytes=%d, errors=%d",
				stats.messagesSent, stats.bytesProcessed, stats.errorCount)
		}
	}
}
