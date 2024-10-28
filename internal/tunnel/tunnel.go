// internal/tunnel/tunnel.go
package tunnel

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type TunnelState int

const (
	StateDisconnected TunnelState = iota
	StateConnecting
	StateConnected
)

type Tunnel struct {
	conn       net.Conn
	hostAddr   string
	state      TunnelState
	stateMutex sync.RWMutex
	sendChan   chan []byte
	ctx        context.Context
	cancel     context.CancelFunc
	maxRetries int
	retryDelay time.Duration
}

func New(hostAddr string) *Tunnel {
	ctx, cancel := context.WithCancel(context.Background())
	t := &Tunnel{
		hostAddr:   hostAddr,
		state:      StateDisconnected,
		sendChan:   make(chan []byte, 1000),
		ctx:        ctx,
		cancel:     cancel,
		maxRetries: 5,           // Default max retries
		retryDelay: time.Second, // Default retry delay
	}

	// Start the connection manager
	go t.connectionManager()
	// Start the send manager
	go t.sendManager()

	return t
}

func (t *Tunnel) setState(state TunnelState) {
	t.stateMutex.Lock()
	defer t.stateMutex.Unlock()
	t.state = state
}

func (t *Tunnel) getState() TunnelState {
	t.stateMutex.RLock()
	defer t.stateMutex.RUnlock()
	return t.state
}

func (t *Tunnel) connectionManager() {
	backoff := time.Second

	for {
		select {
		case <-t.ctx.Done():
			return
		default:
			if t.getState() != StateConnected {
				t.setState(StateConnecting)
				if err := t.connect(); err != nil {
					log.Printf("Connection attempt failed: %v, retrying in %v", err, backoff)
					time.Sleep(backoff)
					// Exponential backoff with max of 1 minute
					backoff = time.Duration(min(backoff.Seconds()*2, 60)) * time.Second
					continue
				}
				backoff = time.Second // Reset backoff on successful connection
				t.setState(StateConnected)
			}
			// Wait for disconnection
			<-t.waitForDisconnection()
		}
	}
}

func (t *Tunnel) connect() error {
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(t.ctx, "tcp", t.hostAddr)
	if err != nil {
		return fmt.Errorf("tunnel connection failed: %w", err)
	}

	t.conn = conn
	return nil
}

func (t *Tunnel) waitForDisconnection() chan struct{} {
	ch := make(chan struct{})

	go func() {
		// Create a small buffer for reading to detect disconnection
		buf := make([]byte, 1)
		for {
			if t.conn == nil {
				break
			}
			t.conn.SetReadDeadline(time.Now().Add(time.Second))
			_, err := t.conn.Read(buf)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				// Connection is dead
				t.setState(StateDisconnected)
				if t.conn != nil {
					t.conn.Close()
					t.conn = nil
				}
				break
			}
		}
		close(ch)
	}()

	return ch
}

func (t *Tunnel) GetDecoder() *json.Decoder {
	if t.conn == nil {
		return nil
	}
	return json.NewDecoder(t.conn)
}

func (t *Tunnel) GetConnection() net.Conn {
	t.stateMutex.RLock()
	defer t.stateMutex.RUnlock()
	return t.conn
}

func (t *Tunnel) sendManager() {
	for {
		select {
		case <-t.ctx.Done():
			return
		case data := <-t.sendChan:
			for retry := 0; retry < t.maxRetries; retry++ {
				if t.getState() != StateConnected {
					time.Sleep(t.retryDelay)
					continue
				}

				if err := t.sendData(data); err != nil {
					log.Printf("Send failed (attempt %d/%d): %v", retry+1, t.maxRetries, err)
					t.setState(StateDisconnected)
					continue
				}
				break
			}
		}
	}
}

func (t *Tunnel) sendData(data []byte) error {
	if t.conn == nil {
		return fmt.Errorf("connection not available")
	}

	_, err := t.conn.Write(append(data, '\n'))
	return err
}

func (t *Tunnel) Send(data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("json marshaling failed: %w", err)
	}

	// Non-blocking send to channel
	select {
	case t.sendChan <- jsonData:
		return nil
	default:
		return fmt.Errorf("send buffer full")
	}
}

func (t *Tunnel) Connect(ctx context.Context) error {
	// This is now just a wrapper for compatibility
	// The actual connection is managed internally
	return nil
}

func (t *Tunnel) Close() error {
	t.cancel() // Stop all goroutines
	if t.conn != nil {
		return t.conn.Close()
	}
	return nil
}

// Helper function
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
