package share

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

var heartbeatLog = logging.Logger("rda.heartbeat")

const (
	// Heartbeat protocol for subnet maintenance
	RDAHeartbeatProtocol = "/celestia/rda/heartbeat/1.0.0"

	// Heartbeat interval - nodes send ping every 10 seconds
	HeartbeatInterval = 10 * time.Second

	// Peer timeout - if no heartbeat for 30 seconds, mark as offline
	PeerTimeoutDuration = 30 * time.Second

	// Cleanup interval - remove stale peers from routing table
	CleanupInterval = 15 * time.Second
)

// RDAHeartbeatMessage is broadcast to maintain subnet connectivity
// Cơ chế: Node gửi ping định kỳ chứa (PeerID, Row, Col, Timestamp)
type RDAHeartbeatMessage struct {
	PeerID    string // Peer's ID
	Row       uint32 // Peer's row position
	Col       uint32 // Peer's column position
	Timestamp int64  // Message timestamp (milliseconds)
	Status    string // "alive" | "leaving"
	GridSize  uint32 // For validation
}

// RDAHeartbeat manages periodic heartbeat/ping broadcasts for subnet maintenance
//
// Tính năng:
// 1. Broadcast ping messages mỗi 10 giây vào subnets mà node đã join
// 2. Listen trên subnet channels để nhận ping từ peers khác
// 3. Update routing table với peer info từ heartbeat
// 4. Loại bỏ offline peers (không có heartbeat > 30 giây)
// 5. Duy trì "Subnet Protocol Good" - trung thực nodes luôn kết nối
type RDAHeartbeat struct {
	host          host.Host
	pubsub        *pubsub.PubSub
	myPeerID      peer.ID
	myRow         uint32
	myCol         uint32
	gridSize      uint32
	routingTable  *RoutingTable
	joinedSubnets map[string]bool        // Track which subnets we've joined
	subscriptions map[string]interface{} // Store subscriptions
	mu            sync.RWMutex

	// Peer tracking for heartbeat timeout
	// PeerID → LastHeartbeatTime
	peerLastSeen map[string]int64
	mu2          sync.RWMutex

	// Control channels
	stopCh    chan struct{}
	startedCh chan bool
	ctx       context.Context
	cancel    context.CancelFunc

	// Callbacks
	onPeerOnline  func(peerID string, addrInfo peer.AddrInfo, row, col uint32)
	onPeerOffline func(peerID string)
}

// NewRDAHeartbeat creates a new heartbeat manager for subnet maintenance
func NewRDAHeartbeat(
	host host.Host,
	pubsub *pubsub.PubSub,
	routingTable *RoutingTable,
	row, col, gridSize uint32,
	onPeerOnline func(peerID string, addrInfo peer.AddrInfo, row, col uint32),
	onPeerOffline func(peerID string),
) *RDAHeartbeat {
	ctx, cancel := context.WithCancel(context.Background())

	return &RDAHeartbeat{
		host:          host,
		pubsub:        pubsub,
		myPeerID:      host.ID(),
		myRow:         row,
		myCol:         col,
		gridSize:      gridSize,
		routingTable:  routingTable,
		joinedSubnets: make(map[string]bool),
		subscriptions: make(map[string]interface{}),
		peerLastSeen:  make(map[string]int64),
		stopCh:        make(chan struct{}),
		startedCh:     make(chan bool, 1),
		ctx:           ctx,
		cancel:        cancel,
		onPeerOnline:  onPeerOnline,
		onPeerOffline: onPeerOffline,
	}
}

// RegisterSubnet registers a subnet for heartbeat monitoring
// Gọi sau khi node JOIN một subnet
func (hb *RDAHeartbeat) RegisterSubnet(subnetID string) error {
	hb.mu.Lock()
	defer hb.mu.Unlock()

	if hb.joinedSubnets[subnetID] {
		return nil // Already registered
	}

	heartbeatLog.Infof(
		"[%s] Registering subnet %s for heartbeat monitoring",
		hb.myPeerID.String()[:8], subnetID,
	)

	hb.joinedSubnets[subnetID] = true
	return nil
}

// Start begins the heartbeat mechanism
// - Broadcast heartbeat messages mỗi 10 giây
// - Listen cho heartbeat từ peers khác
// - Cleanup stale peers mỗi 15 giây
func (hb *RDAHeartbeat) Start(ctx context.Context) error {
	hb.mu.Lock()
	defer hb.mu.Unlock()

	heartbeatLog.Infof(
		"[%s] Starting RDA Heartbeat - row=%d, col=%d, grid=%d",
		hb.myPeerID.String()[:8], hb.myRow, hb.myCol, hb.gridSize,
	)

	// Start heartbeat broadcaster (10s interval)
	go hb.heartbeatBroadcaster()

	// Start heartbeat listener for each joined subnet
	go hb.heartbeatListeners()

	// Start cleanup worker (remove stale peers)
	go hb.cleanupStalepeers()

	// Start stale peer detector (offline detection)
	go hb.detectOfflinePeers()

	hb.startedCh <- true
	heartbeatLog.Infof("[%s] Heartbeat manager started ✓", hb.myPeerID.String()[:8])

	return nil
}

// Stop gracefully shuts down the heartbeat mechanism
func (hb *RDAHeartbeat) Stop() error {
	heartbeatLog.Infof("[%s] Stopping RDA Heartbeat", hb.myPeerID.String()[:8])

	hb.mu.Lock()
	// Send leaving message
	hb.broadcastHeartbeat("leaving")

	// Close all subscriptions
	for subnetID := range hb.subscriptions {
		// Safely close subscription if it's a cancellable interface
		if sub, ok := hb.subscriptions[subnetID]; ok {
			if cancellable, ok := sub.(interface{ Cancel() }); ok {
				cancellable.Cancel()
			}
		}
		heartbeatLog.Debugf("[%s] Closed subscription for subnet %s", hb.myPeerID.String()[:8], subnetID)
	}
	hb.subscriptions = make(map[string]interface{})
	hb.mu.Unlock()

	hb.cancel()
	close(hb.stopCh)

	return nil
}

// heartbeatBroadcaster broadcasts heartbeat messages every 10 seconds
// Broadcast quy trình:
// 1. Mỗi 10s, tạo RDAHeartbeatMessage (PeerID, Row, Col, Timestamp)
// 2. Gửi vào cả row subnet và column subnet
// 3. Trung thực nodes sẽ nhận được heartbeat từ mọi nodes cùng row/col
func (hb *RDAHeartbeat) heartbeatBroadcaster() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-hb.stopCh:
			return
		case <-hb.ctx.Done():
			return
		case <-ticker.C:
			hb.broadcastHeartbeat("alive")
		}
	}
}

// broadcastHeartbeat broadcasts a heartbeat message to all joined subnets
func (hb *RDAHeartbeat) broadcastHeartbeat(status string) {
	hb.mu.RLock()
	subnets := make([]string, 0, len(hb.joinedSubnets))
	for subnetID := range hb.joinedSubnets {
		subnets = append(subnets, subnetID)
	}
	hb.mu.RUnlock()

	msg := RDAHeartbeatMessage{
		PeerID:    hb.myPeerID.String(),
		Row:       hb.myRow,
		Col:       hb.myCol,
		Timestamp: time.Now().UnixNano() / 1e6,
		Status:    status,
		GridSize:  hb.gridSize,
	}

	msgBytes, err := json.Marshal(msg)
	if err != nil {
		heartbeatLog.Warnf("[%s] Failed to marshal heartbeat: %v", hb.myPeerID.String()[:8], err)
		return
	}

	// Broadcast to each joined subnet
	for _, subnetID := range subnets {
		topic, err := hb.pubsub.Join(subnetID)
		if err != nil {
			heartbeatLog.Warnf(
				"[%s] Failed to join subnet %s for heartbeat: %v",
				hb.myPeerID.String()[:8], subnetID, err,
			)
			continue
		}

		err = topic.Publish(hb.ctx, msgBytes)
		if err != nil {
			heartbeatLog.Debugf(
				"[%s] Failed to publish heartbeat to %s: %v",
				hb.myPeerID.String()[:8], subnetID, err,
			)
		}

		topic.Close()
	}

	heartbeatLog.Debugf(
		"[%s] Broadcast heartbeat to %d subnets - status=%s",
		hb.myPeerID.String()[:8], len(subnets), status,
	)
}

// heartbeatListeners listens for heartbeat messages on subscribed subnets
// Receive quy trình:
// 1. Subscribe vào row + col subnets
// 2. Nghe mọi heartbeat từ peers khác
// 3. Update "last seen" timestamp
// 4. Add/update vào routing table
func (hb *RDAHeartbeat) heartbeatListeners() {
	// We need to be listening continuously, so subscribe to row and col subnets
	// But we'll start listening when subnets are registered

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	registeredSubnets := make(map[string]bool)

	for {
		select {
		case <-hb.stopCh:
			return
		case <-hb.ctx.Done():
			return
		case <-ticker.C:
			hb.mu.RLock()
			currentSubnets := make(map[string]bool)
			for subnetID := range hb.joinedSubnets {
				currentSubnets[subnetID] = true
			}
			hb.mu.RUnlock()

			// Subscribe to new subnets
			for subnetID := range currentSubnets {
				if !registeredSubnets[subnetID] {
					hb.subscribeToSubnet(subnetID)
					registeredSubnets[subnetID] = true
				}
			}

			// Receive messages from subscriptions
			hb.receiveHeartbeats()
		}
	}
}

// subscribeToSubnet subscribes to heartbeat messages on a specific subnet
func (hb *RDAHeartbeat) subscribeToSubnet(subnetID string) {
	hb.mu.Lock()
	defer hb.mu.Unlock()

	if hb.subscriptions[subnetID] != nil {
		return // Already subscribed
	}

	topic, err := hb.pubsub.Join(subnetID)
	if err != nil {
		heartbeatLog.Warnf(
			"[%s] Failed to join topic %s: %v",
			hb.myPeerID.String()[:8], subnetID, err,
		)
		return
	}

	sub, err := topic.Subscribe()
	if err != nil {
		heartbeatLog.Warnf(
			"[%s] Failed to subscribe to topic %s: %v",
			hb.myPeerID.String()[:8], subnetID, err,
		)
		topic.Close()
		return
	}

	hb.subscriptions[subnetID] = sub

	heartbeatLog.Infof(
		"[%s] Subscribed to heartbeat on subnet %s",
		hb.myPeerID.String()[:8], subnetID,
	)
}

// receiveHeartbeats receives heartbeat messages from subscribed subnets
func (hb *RDAHeartbeat) receiveHeartbeats() {
	hb.mu.RLock()
	subscriptions := make(map[string]interface{})
	for subnetID, sub := range hb.subscriptions {
		subscriptions[subnetID] = sub
	}
	hb.mu.RUnlock()

	for subnetID, subInterface := range subscriptions {
		// Type assert to get the actual subscription
		sub, ok := subInterface.(*pubsub.Subscription)
		if !ok {
			continue
		}

		msg, err := sub.Next(hb.ctx)
		if err != nil {
			continue
		}

		var heartbeat RDAHeartbeatMessage
		err = json.Unmarshal(msg.Data, &heartbeat)
		if err != nil {
			heartbeatLog.Debugf("[%s] Failed to parse heartbeat: %v", hb.myPeerID.String()[:8], err)
			continue
		}

		// Ignore own heartbeat
		if heartbeat.PeerID == hb.myPeerID.String() {
			continue
		}

		hb.handleHeartbeat(&heartbeat, subnetID)
	}
}

// handleHeartbeat processes a received heartbeat message
// Updates routing table và tracks peer online status
func (hb *RDAHeartbeat) handleHeartbeat(msg *RDAHeartbeatMessage, subnetID string) {
	heartbeatLog.Debugf(
		"[%s] Received heartbeat from %s (row=%d, col=%d, status=%s) on subnet %s",
		hb.myPeerID.String()[:8], msg.PeerID[:8], msg.Row, msg.Col, msg.Status, subnetID,
	)

	// Update last seen time
	hb.mu2.Lock()
	hb.peerLastSeen[msg.PeerID] = time.Now().Unix()
	hb.mu2.Unlock()

	// Handle peer leaving
	if msg.Status == "leaving" {
		heartbeatLog.Infof("[%s] Peer %s is leaving", hb.myPeerID.String()[:8], msg.PeerID[:8])
		if hb.onPeerOffline != nil {
			hb.onPeerOffline(msg.PeerID)
		}
		return
	}

	// Add/update peer in routing table
	peerID, err := peer.Decode(msg.PeerID)
	if err != nil {
		heartbeatLog.Warnf("[%s] Failed to decode peer ID: %v", hb.myPeerID.String()[:8], err)
		return
	}

	// Get peer addresses from host's peerstore
	addrs := hb.host.Peerstore().Addrs(peerID)
	if len(addrs) == 0 {
		// If no addresses known, skip for now
		heartbeatLog.Debugf("[%s] No addresses known for peer %s", hb.myPeerID.String()[:8], msg.PeerID[:8])
		return
	}

	addrInfo := peer.AddrInfo{
		ID:    peerID,
		Addrs: addrs,
	}

	// Update routing table
	hb.routingTable.AddPeer(msg.PeerID, addrInfo, msg.Row, msg.Col)

	// Callback
	if hb.onPeerOnline != nil {
		hb.onPeerOnline(msg.PeerID, addrInfo, msg.Row, msg.Col)
	}
}

// cleanupStalepeers regularly removes offline peers from routing table
// Cleanup quy trình:
// 1. Mỗi 15 giây, scan peers trong routing table
// 2. Nếu peer không có heartbeat > 30s, xóa khỏi routing table
// 3. Trigger callback onPeerOffline
func (hb *RDAHeartbeat) cleanupStalepeers() {
	ticker := time.NewTicker(CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-hb.stopCh:
			return
		case <-hb.ctx.Done():
			return
		case <-ticker.C:
			hb.removeStalePeers()
		}
	}
}

// removeStalePeers removes peers that haven't sent heartbeat for > 30 seconds
func (hb *RDAHeartbeat) removeStalePeers() {
	hb.mu2.Lock()
	now := time.Now().Unix()
	stalePeers := make([]string, 0)

	for peerID, lastSeen := range hb.peerLastSeen {
		if now-lastSeen > int64(PeerTimeoutDuration.Seconds()) {
			stalePeers = append(stalePeers, peerID)
		}
	}

	for _, peerID := range stalePeers {
		delete(hb.peerLastSeen, peerID)
	}
	hb.mu2.Unlock()

	if len(stalePeers) > 0 {
		peerIDStr := hb.myPeerID.String()
		displayStr := peerIDStr
		if len(peerIDStr) > 8 {
			displayStr = peerIDStr[:8]
		}
		heartbeatLog.Infof(
			"[%s] Removed %d stale peers from tracking",
			displayStr, len(stalePeers),
		)
	}
}

// detectOfflinePeers detects and reports peers that go offline
func (hb *RDAHeartbeat) detectOfflinePeers() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	lastKnownPeers := make(map[string]bool)

	for {
		select {
		case <-hb.stopCh:
			return
		case <-hb.ctx.Done():
			return
		case <-ticker.C:
			hb.mu2.RLock()
			currentPeers := make(map[string]bool)
			now := time.Now().Unix()

			for peerID, lastSeen := range hb.peerLastSeen {
				if now-lastSeen <= int64(PeerTimeoutDuration.Seconds()) {
					currentPeers[peerID] = true
				}
			}
			hb.mu2.RUnlock()

			// Find peers that went offline
			for peerID := range lastKnownPeers {
				if !currentPeers[peerID] {
					heartbeatLog.Infof(
						"[%s] Peer %s detected as offline",
						hb.myPeerID.String()[:8], peerID[:8],
					)
					if hb.onPeerOffline != nil {
						hb.onPeerOffline(peerID)
					}
				}
			}

			lastKnownPeers = currentPeers
		}
	}
}

// GetOnlinePeersForRow returns all online peers in a specific row
func (hb *RDAHeartbeat) GetOnlinePeersForRow(row uint32) []peer.AddrInfo {
	peers := hb.routingTable.GetRowPeers(row, 1000, "")
	result := make([]peer.AddrInfo, 0, len(peers))

	hb.mu2.RLock()
	now := time.Now().Unix()
	defer hb.mu2.RUnlock()

	for _, entry := range peers {
		// Check if still online (has recent heartbeat)
		if lastSeen, ok := hb.peerLastSeen[entry.PeerID]; ok {
			if now-lastSeen <= int64(PeerTimeoutDuration.Seconds()) {
				peerID, _ := peer.Decode(entry.PeerID)
				result = append(result, peer.AddrInfo{
					ID:    peerID,
					Addrs: entry.AddrInfo.Addrs,
				})
			}
		}
	}

	return result
}

// GetOnlinePeersForCol returns all online peers in a specific column
func (hb *RDAHeartbeat) GetOnlinePeersForCol(col uint32) []peer.AddrInfo {
	peers := hb.routingTable.GetColPeers(col, 1000, "")
	result := make([]peer.AddrInfo, 0, len(peers))

	hb.mu2.RLock()
	now := time.Now().Unix()
	defer hb.mu2.RUnlock()

	for _, entry := range peers {
		// Check if still online (has recent heartbeat)
		if lastSeen, ok := hb.peerLastSeen[entry.PeerID]; ok {
			if now-lastSeen <= int64(PeerTimeoutDuration.Seconds()) {
				peerID, _ := peer.Decode(entry.PeerID)
				result = append(result, peer.AddrInfo{
					ID:    peerID,
					Addrs: entry.AddrInfo.Addrs,
				})
			}
		}
	}

	return result
}

// GetHeartbeatStats returns statistics about heartbeat system
func (hb *RDAHeartbeat) GetHeartbeatStats() map[string]interface{} {
	hb.mu.RLock()
	numSubnets := len(hb.joinedSubnets)
	hb.mu.RUnlock()

	hb.mu2.RLock()
	now := time.Now().Unix()
	onlinePeers := 0
	for _, lastSeen := range hb.peerLastSeen {
		if now-lastSeen <= int64(PeerTimeoutDuration.Seconds()) {
			onlinePeers++
		}
	}
	totalTrackedPeers := len(hb.peerLastSeen)
	hb.mu2.RUnlock()

	return map[string]interface{}{
		"node_id":             hb.myPeerID.String(),
		"position":            fmt.Sprintf("row=%d, col=%d", hb.myRow, hb.myCol),
		"joined_subnets":      numSubnets,
		"heartbeat_interval":  HeartbeatInterval.String(),
		"peer_timeout":        PeerTimeoutDuration.String(),
		"total_tracked_peers": totalTrackedPeers,
		"online_peers":        onlinePeers,
		"offline_peers":       totalTrackedPeers - onlinePeers,
	}
}
