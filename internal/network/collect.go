// internal/network/collector.go
package network

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/shirou/gopsutil/v3/net"
)

const (
	packetQueue    = "network:packets:queue"   // Raw packets waiting to be processed
	processedQueue = "network:processed:queue" // Processed metrics ready for client consumption
	batchSize      = 1000
	maxQueueLength = 10000
)

type Collector struct {
	redisClient   *redis.Client
	metrics       chan NetworkMetrics
	interfaces    []string
	lastProcessed time.Time
	statsInterval time.Duration
}

type NetworkMetrics struct {
	Timestamp      time.Time        `json:"timestamp"`
	Packets        []PacketInfo     `json:"packets"`
	Connections    []ConnectionInfo `json:"connections"`
	InterfaceStats []InterfaceStats `json:"interface_stats"`
}

type PacketInfo struct {
	Timestamp   time.Time `json:"timestamp"`
	Length      int       `json:"length"`
	Protocol    string    `json:"protocol"`
	SrcIP       string    `json:"src_ip"`
	DstIP       string    `json:"dst_ip"`
	SrcPort     uint16    `json:"src_port"`
	DstPort     uint16    `json:"dst_port"`
	TCPFlags    string    `json:"tcp_flags,omitempty"`
	ICMPType    uint8     `json:"icmp_type,omitempty"`
	PayloadSize int       `json:"payload_size"`
}

type ConnectionInfo struct {
	Protocol    string `json:"protocol"`
	LocalIP     string `json:"local_ip"`
	LocalPort   uint32 `json:"local_port"`
	RemoteIP    string `json:"remote_ip"`
	RemotePort  uint32 `json:"remote_port"`
	State       string `json:"state"`
	PID         int32  `json:"pid"`
	ProcessName string `json:"process_name,omitempty"`
}

type InterfaceStats struct {
	Name        string `json:"name"`
	BytesSent   uint64 `json:"bytes_sent"`
	BytesRecv   uint64 `json:"bytes_recv"`
	PacketsSent uint64 `json:"packets_sent"`
	PacketsRecv uint64 `json:"packets_recv"`
	ErrorsIn    uint64 `json:"errors_in"`
	ErrorsOut   uint64 `json:"errors_out"`
	DropIn      uint64 `json:"drop_in"`
	DropOut     uint64 `json:"drop_out"`
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

	interfaces, err := pcap.FindAllDevs()
	if err != nil {
		return nil, fmt.Errorf("failed to get interfaces: %w", err)
	}

	var ifaceNames []string
	for _, iface := range interfaces {
		if isNetworkInterface(iface.Name) {
			ifaceNames = append(ifaceNames, iface.Name)
		}
	}

	return &Collector{
		redisClient:   rdb,
		metrics:       make(chan NetworkMetrics, 100),
		interfaces:    ifaceNames,
		lastProcessed: time.Now(),
		statsInterval: 5 * time.Second,
	}, nil
}

func (c *Collector) Start(ctx context.Context) {
	log.Println("Starting network collector...")

	// Start packet capture for each interface
	for _, iface := range c.interfaces {
		go c.capturePackets(ctx, iface)
	}

	// Start continuous batch processor
	go c.startBatchProcessor(ctx)

	// Start periodic sender for batched metrics
	go c.startPeriodicSender(ctx)
}

func (c *Collector) capturePackets(ctx context.Context, iface string) {
	handle, err := pcap.OpenLive(iface, 1600, false, pcap.BlockForever)
	if err != nil {
		log.Printf("Failed to open interface %s: %v", iface, err)
		return
	}
	defer handle.Close()

	err = handle.SetBPFFilter("tcp or udp or icmp")
	if err != nil {
		log.Printf("Failed to set BPF filter on %s: %v", iface, err)
	}

	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	for {
		select {
		case <-ctx.Done():
			return
		case packet := <-packetSource.Packets():
			if packet != nil {
				if err := c.processPacket(ctx, packet); err != nil {
					log.Printf("Failed to process packet: %v", err)
				}
			}
		}
	}
}

func (c *Collector) processPacket(ctx context.Context, packet gopacket.Packet) error {
	ipLayer := packet.Layer(layers.LayerTypeIPv4)
	if ipLayer == nil {
		return nil
	}

	ip, _ := ipLayer.(*layers.IPv4)
	packetInfo := PacketInfo{
		Timestamp: time.Now(),
		Length:    len(packet.Data()),
		Protocol:  ip.Protocol.String(),
		SrcIP:     ip.SrcIP.String(),
		DstIP:     ip.DstIP.String(),
	}

	// Set default payload size
	packetInfo.PayloadSize = 0
	if appLayer := packet.ApplicationLayer(); appLayer != nil {
		packetInfo.PayloadSize = len(appLayer.Payload())
	}

	if tcpLayer := packet.Layer(layers.LayerTypeTCP); tcpLayer != nil {
		tcp, _ := tcpLayer.(*layers.TCP)
		packetInfo.SrcPort = uint16(tcp.SrcPort)
		packetInfo.DstPort = uint16(tcp.DstPort)
		packetInfo.TCPFlags = fmt.Sprintf("SYN:%t ACK:%t FIN:%t RST:%t",
			tcp.SYN, tcp.ACK, tcp.FIN, tcp.RST)
	}

	if udpLayer := packet.Layer(layers.LayerTypeUDP); udpLayer != nil {
		udp, _ := udpLayer.(*layers.UDP)
		packetInfo.SrcPort = uint16(udp.SrcPort)
		packetInfo.DstPort = uint16(udp.DstPort)
	}

	if icmpLayer := packet.Layer(layers.LayerTypeICMPv4); icmpLayer != nil {
		icmp, _ := icmpLayer.(*layers.ICMPv4)
		packetInfo.ICMPType = uint8(icmp.TypeCode >> 8)
	}

	data, err := json.Marshal(packetInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal packet info: %w", err)
	}

	// Store raw packet in the packet queue
	pipe := c.redisClient.Pipeline()
	pipe.LPush(ctx, packetQueue, data)
	pipe.LTrim(ctx, packetQueue, 0, maxQueueLength-1)
	_, err = pipe.Exec(ctx)

	return err
}

func (c *Collector) startBatchProcessor(ctx context.Context) {
	log.Println("Starting batch processor...")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := c.processNextBatch(ctx); err != nil {
				if err != redis.Nil {
					log.Printf("Batch processing error: %v", err)
					time.Sleep(time.Second) // Back off on errors
				}
			}
		}
	}
}

func (c *Collector) processNextBatch(ctx context.Context) error {
	// Atomically get and remove a batch of packets
	var packets []PacketInfo

	// Use RPOP for atomic removal of processed packets
	results, err := c.redisClient.LPopCount(ctx, packetQueue, batchSize).Result()
	if err == redis.Nil || len(results) == 0 {
		time.Sleep(100 * time.Millisecond) // Prevent tight loop when no packets
		return redis.Nil
	}
	if err != nil {
		return fmt.Errorf("failed to pop packets: %w", err)
	}

	// Process the batch
	for _, result := range results {
		var packet PacketInfo
		if err := json.Unmarshal([]byte(result), &packet); err != nil {
			log.Printf("Failed to unmarshal packet: %v", err)
			continue
		}
		packets = append(packets, packet)
	}

	if len(packets) == 0 {
		return nil
	}

	// Collect system metrics only periodically
	var connections []ConnectionInfo
	var interfaceStats []InterfaceStats

	if time.Since(c.lastProcessed) >= c.statsInterval {
		var err error
		connections, err = c.getConnections()
		if err != nil {
			log.Printf("Failed to get connections: %v", err)
		}

		interfaceStats, err = c.getInterfaceStats()
		if err != nil {
			log.Printf("Failed to get interface stats: %v", err)
		}
		c.lastProcessed = time.Now()
	}

	metrics := NetworkMetrics{
		Timestamp:      time.Now(),
		Packets:        packets,
		Connections:    connections,
		InterfaceStats: interfaceStats,
	}

	// Store processed metrics
	data, err := json.Marshal(metrics)
	if err != nil {
		return fmt.Errorf("failed to marshal metrics: %w", err)
	}

	// Store in processed queue for batch sending
	pipe := c.redisClient.Pipeline()
	pipe.LPush(ctx, processedQueue, data)
	pipe.LTrim(ctx, processedQueue, 0, maxQueueLength-1)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to store processed metrics: %w", err)
	}

	// Send to real-time channel
	select {
	case c.metrics <- metrics:
	default:
		log.Printf("Metrics channel full, skipping real-time update")
	}

	return nil
}

func (c *Collector) startPeriodicSender(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Get all metrics from processed queue
			results, err := c.redisClient.LRange(ctx, processedQueue, 0, -1).Result()
			if err != nil {
				log.Printf("Failed to get processed metrics: %v", err)
				continue
			}

			if len(results) > 0 {
				// Clear processed queue after retrieving
				if err := c.redisClient.Del(ctx, processedQueue).Err(); err != nil {
					log.Printf("Failed to clear processed queue: %v", err)
				}
				log.Printf("Sending batch of %d processed metrics sets", len(results))
				// Here you would send the batch to your client
			}
		}
	}
}

func (c *Collector) getConnections() ([]ConnectionInfo, error) {
	conns, err := net.Connections("all")
	if err != nil {
		return nil, err
	}

	var connections []ConnectionInfo
	for _, conn := range conns {
		connections = append(connections, ConnectionInfo{
			Protocol:   protocolToString(conn.Type),
			LocalIP:    conn.Laddr.IP,
			LocalPort:  uint32(conn.Laddr.Port),
			RemoteIP:   conn.Raddr.IP,
			RemotePort: uint32(conn.Raddr.Port),
			State:      conn.Status,
			PID:        conn.Pid,
		})
	}

	return connections, nil
}

func (c *Collector) getInterfaceStats() ([]InterfaceStats, error) {
	stats, err := net.IOCounters(true)
	if err != nil {
		return nil, err
	}

	var interfaceStats []InterfaceStats
	for _, stat := range stats {
		interfaceStats = append(interfaceStats, InterfaceStats{
			Name:        stat.Name,
			BytesSent:   stat.BytesSent,
			BytesRecv:   stat.BytesRecv,
			PacketsSent: stat.PacketsSent,
			PacketsRecv: stat.PacketsRecv,
			ErrorsIn:    stat.Errin,
			ErrorsOut:   stat.Errout,
			DropIn:      stat.Dropin,
			DropOut:     stat.Dropout,
		})
	}

	return interfaceStats, nil
}

func isNetworkInterface(name string) bool {
	networkPrefixes := []string{
		"eth", "en", "wl", "ww", "wlan", "veth", "docker",
		"br-", "bond", "tun", "tap", "vmnet",
	}

	skipPrefixes := []string{
		"bluetooth", "dbus", "lo", "ham", "vmnet",
		"docker", "veth", "br-",
	}

	for _, prefix := range skipPrefixes {
		if strings.HasPrefix(name, prefix) {
			return false
		}
	}

	for _, prefix := range networkPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}

	return false
}

func protocolToString(p uint32) string {
	switch p {
	case 1:
		return "tcp"
	case 2:
		return "udp"
	case 3:
		return "tcp6"
	case 4:
		return "udp6"
	default:
		return "unknown"
	}
}

func (c *Collector) Metrics() <-chan NetworkMetrics {
	return c.metrics
}
