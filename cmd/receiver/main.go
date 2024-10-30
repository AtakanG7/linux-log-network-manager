// cmd/receiver/main.go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"
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

type LogFile struct {
	Path      string    `json:"path"`
	Size      int64     `json:"size"`
	ModTime   time.Time `json:"mod_time"`
	IsGzipped bool      `json:"is_gzipped"`
}

type LogSearchCommand struct {
	Files    []string `json:"files"`
	Keywords []string `json:"keywords"`
}

func main() {
	log.Println("Starting metrics receiver...")

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to start listener: %v", err)
	}
	defer listener.Close()

	log.Println("✓ Listening on :8080")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Connection accept error: %v", err)
			continue
		}
		log.Printf("New connection from: %s", conn.RemoteAddr())
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	decoder := json.NewDecoder(conn)

	// Initialize random seed
	rand.Seed(time.Now().UnixNano())

	for {
		// Read incoming message
		var msg Message
		fmt.Print(msg)
		if err := decoder.Decode(&msg); err != nil {
			log.Printf("Connection closed or error: %v", err)
			return
		}

		// Handle message based on type
		switch msg.Type {
		case TypeLogList:
			// Parse the file list
			var files []LogFile
			if data, err := json.Marshal(msg.Payload); err == nil {
				if err := json.Unmarshal(data, &files); err == nil {
					log.Printf("Received file list with %d files", len(files))

					// Select random files
					selectedFiles := selectRandomFiles(files, 3)
					if len(selectedFiles) > 0 {
						// Create search command
						searchMsg := Message{
							Type: TypeLogSearch,
							Payload: LogSearchCommand{
								Files:    selectedFiles,
								Keywords: []string{""},
							},
						}

						// Marshal to JSON with newline
						var buf bytes.Buffer
						encoder := json.NewEncoder(&buf)
						if err := encoder.Encode(searchMsg); err != nil {
							log.Printf("Failed to encode search message: %v", err)
							continue
						}

						// Write the complete message
						if _, err := conn.Write(buf.Bytes()); err != nil {
							log.Printf("Failed to send search command: %v", err)
							continue
						}

						log.Printf("Successfully sent search command for files: %v", selectedFiles)
					}
				} else {
					log.Printf("Failed to parse file list: %v", err)
				}
			}
		case TypeLogData:
			log.Printf("Received log data")
		case TypeMetrics:
			log.Printf("Received metrics data")
		}
	}
}

func selectRandomFiles(files []LogFile, count int) []string {
	if len(files) == 0 {
		return nil
	}

	filesCopy := make([]LogFile, len(files))
	copy(filesCopy, files)
	rand.Shuffle(len(filesCopy), func(i, j int) {
		filesCopy[i], filesCopy[j] = filesCopy[j], filesCopy[i]
	})

	numFiles := min(count, len(filesCopy))
	selectedFiles := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		selectedFiles[i] = filesCopy[i].Path
	}

	return selectedFiles
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
