// cmd/receiver/main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
)

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

	for {
		var data interface{}
		if err := decoder.Decode(&data); err != nil {
			log.Printf("Connection closed or error: %v", err)
			return
		}

		prettyJSON, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			log.Printf("Error formatting JSON: %v", err)
			continue
		}

		fmt.Printf("\nReceived Metrics:\n%s\n", string(prettyJSON))

	}
}
