// internal/network/network.go
package network

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/shirou/gopsutil/v3/net"
)

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

type NetworkMetrics struct {
	Timestamp      time.Time        `json:"timestamp"`
	Packets        []PacketInfo     `json:"packets"`
	Connections    []ConnectionInfo `json:"connections"`
	InterfaceStats []InterfaceStats `json:"interface_stats"`
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

type Collector struct {
	redisClient  *redis.Client
	metrics      chan NetworkMetrics
	packetBuffer []PacketInfo
	bufferMutex  sync.Mutex
	interfaces   []string
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
		redisClient:  rdb,
		metrics:      make(chan NetworkMetrics, 100),
		packetBuffer: make([]PacketInfo, 0, 1000),
		interfaces:   ifaceNames,
	}, nil
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

func (c *Collector) processPacket(packet gopacket.Packet) {
	ipLayer := packet.Layer(layers.LayerTypeIPv4)
	if ipLayer == nil {
		return
	}

	ip, _ := ipLayer.(*layers.IPv4)
	packetInfo := PacketInfo{
		Timestamp: time.Now(),
		Length:    len(packet.Data()),
		Protocol:  ip.Protocol.String(),
		SrcIP:     ip.SrcIP.String(),
		DstIP:     ip.DstIP.String(),
	}

	// Set default payload size to 0
	packetInfo.PayloadSize = 0

	// Safely check application layer
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

	c.bufferMutex.Lock()
	c.packetBuffer = append(c.packetBuffer, packetInfo)
	c.bufferMutex.Unlock()
}

func (c *Collector) capturePackets(ctx context.Context, iface string) {
	handle, err := pcap.OpenLive(iface, 1600, false, pcap.BlockForever)
	if err != nil {
		log.Printf("Failed to open interface %s: %v", iface, err)
		return
	}
	defer handle.Close()

	// Set a simple filter to reduce noise
	err = handle.SetBPFFilter("tcp or udp or icmp")
	if err != nil {
		log.Printf("Failed to set BPF filter on %s: %v", iface, err)
		// Continue anyway
	}

	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	packets := packetSource.Packets()

	for {
		select {
		case <-ctx.Done():
			return
		case packet := <-packets:
			if packet != nil {
				c.processPacket(packet)
			}
		}
	}
}

func (c *Collector) collectConnections() ([]ConnectionInfo, error) {
	conns, err := net.Connections("all")
	if err != nil {
		return nil, err
	}

	var connections []ConnectionInfo
	for _, conn := range conns {
		connections = append(connections, ConnectionInfo{
			Protocol:   protocolToString(conn.Type), // Convert uint32 to string
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

func (c *Collector) collectInterfaceStats() ([]InterfaceStats, error) {
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

func (c *Collector) Start(ctx context.Context) {
	// Start packet capture for each interface
	for _, iface := range c.interfaces {
		go c.capturePackets(ctx, iface)
	}

	// Metrics collection ticker
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Collect current metrics
			connections, err := c.collectConnections()
			if err != nil {
				log.Printf("Failed to collect connections: %v", err)
			}

			interfaceStats, err := c.collectInterfaceStats()
			if err != nil {
				log.Printf("Failed to collect interface stats: %v", err)
			}

			// Get and clear packet buffer
			c.bufferMutex.Lock()
			packets := make([]PacketInfo, len(c.packetBuffer))
			copy(packets, c.packetBuffer)
			c.packetBuffer = c.packetBuffer[:0]
			c.bufferMutex.Unlock()

			metrics := NetworkMetrics{
				Timestamp:      time.Now(),
				Packets:        packets,
				Connections:    connections,
				InterfaceStats: interfaceStats,
			}

			data, err := json.Marshal(metrics)
			if err != nil {
				log.Printf("Failed to marshal metrics: %v", err)
				continue
			}

			err = c.redisClient.LPush(ctx, "network_metrics", data).Err()
			if err != nil {
				log.Printf("Failed to store metrics in Redis: %v", err)
				continue
			}

			// Trim the list to keep last 1000 entries
			c.redisClient.LTrim(ctx, "network_metrics", 0, 999)

			// Send to channel for real-time updates
			c.metrics <- metrics
		}
	}
}

func (c *Collector) Metrics() <-chan NetworkMetrics {
	return c.metrics
}

func (c *Collector) ConsumeQueue(ctx context.Context) (<-chan NetworkMetrics, error) {
	outChan := make(chan NetworkMetrics, 100)

	go func() {
		defer close(outChan)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				result, err := c.redisClient.BRPop(ctx, 0, "network_metrics").Result()
				if err != nil {
					if err != context.Canceled {
						log.Printf("Redis consumption error: %v", err)
					}
					continue
				}

				if len(result) < 2 {
					continue
				}

				var metrics NetworkMetrics
				if err := json.Unmarshal([]byte(result[1]), &metrics); err != nil {
					log.Printf("Failed to unmarshal metrics: %v", err)
					continue
				}

				outChan <- metrics
			}
		}
	}()

	return outChan, nil
}
