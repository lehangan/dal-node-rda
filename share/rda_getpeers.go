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
	"github.com/libp2p/go-libp2p/core/protocol"
)

var getpeersLog = logging.Logger("rda.getpeers")

const (
	// GETPEERS protocol for querying subnet peers
	RDAGetPeersProtocol protocol.ID = "/celestia/rda/getpeers/1.0.0"

	// GETPEERS request/response types
	GetPeersRequest  = "get_peers"
	GetPeersResponse = "get_peers_resp"
)

// GetPeersRequest yêu cầu danh sách peers trong một subnet
type GetPeersRequestMsg struct {
	RequestID string // Unique request ID for tracing
	NodeID    string // Requesting node's peer ID
	SubnetID  string // Subnet ID: "rda/row/{R}" or "rda/col/{C}"
	Timestamp int64  // Request timestamp
}

// GetPeersResponse trả về danh sách peers trong subnet
type GetPeersResponseMsg struct {
	RequestID  string        // Match request ID for correlation
	Success    bool          // Whether request succeeded
	Error      string        // Error message if failed
	SubnetID   string        // Subnet ID
	Peers      []GetPeerInfo // List of discovered peers
	PeerCount  int           // Total peers in response
	LatencyMs  int64         // Response latency
	Timestamp  int64         // Response timestamp
	SourceType string        // "bootstrap" or "pubsub" or "combined"
}

// GetPeerInfo thông tin về một peer trong subnet
type GetPeerInfo struct {
	PeerID    string   // Peer ID string
	Addresses []string // Multiaddrs as strings
	LastSeen  int64    // Last seen timestamp
	Source    string   // "bootstrap" or "pubsub"
}

// RDAGetPeers thực hiện GETPEERS operation để khám phá peers
// Kết hợp hai nguồn:
// 1. Query Bootstrap Nodes (RPC/Stream)
// 2. Listen trên PubSub channel (GossipSub)
type RDAGetPeers struct {
	requestID        string
	host             host.Host
	pubsub           *pubsub.PubSub
	bootstrapNodeIDs []peer.ID // Bootstrap nodes to query
	subnetID         string    // Target subnet (e.g., "rda/row/5")
	myPeerID         peer.ID
	timeout          time.Duration
	bootstrapTimeout time.Duration

	mu               sync.RWMutex
	discoveredPeers  map[string]*GetPeerInfo // PeerID → info (for dedup)
	pubsubPeers      map[string]*GetPeerInfo // Peers from PubSub
	bootstrapPeerMap map[string]*GetPeerInfo // Peers from Bootstrap nodes
	pubsubCallbacks  []chan *GetPeerInfo     // Channels for pubsub updates
}

// NewRDAGetPeers tạo một GETPEERS handler mới
func NewRDAGetPeers(
	host host.Host,
	pubsub *pubsub.PubSub,
	bootstrapPeerList []peer.ID,
	myPeerID peer.ID,
	subnetID string,
	timeout time.Duration,
) *RDAGetPeers {
	return &RDAGetPeers{
		requestID:        fmt.Sprintf("getpeers-%d-%d", time.Now().UnixNano()/1e6, 0),
		host:             host,
		pubsub:           pubsub,
		bootstrapNodeIDs: bootstrapPeerList,
		subnetID:         subnetID,
		myPeerID:         myPeerID,
		timeout:          timeout,
		bootstrapTimeout: 5 * time.Second,
		discoveredPeers:  make(map[string]*GetPeerInfo),
		pubsubPeers:      make(map[string]*GetPeerInfo),
		bootstrapPeerMap: make(map[string]*GetPeerInfo),
		pubsubCallbacks:  make([]chan *GetPeerInfo, 0),
	}
}

// Execute thực hiện GETPEERS operation
// Truy vấn Bootstrap Nodes và PubSub đồng thời
func (gp *RDAGetPeers) Execute(ctx context.Context) (*GetPeersResponseMsg, error) {
	startTime := time.Now()

	response := &GetPeersResponseMsg{
		RequestID: gp.requestID,
		SubnetID:  gp.subnetID,
		Peers:     []GetPeerInfo{},
		Success:   false,
		Timestamp: startTime.UnixNano() / 1e6,
	}

	getpeersLog.Infof(
		"[%s] GETPEERS START - subnet=%s, bootstrap_peers=%d",
		gp.requestID, gp.subnetID, len(gp.bootstrapNodeIDs),
	)

	// Phase 1: Query Bootstrap Nodes in parallel
	bootstrapCtx, bootstrapCancel := context.WithTimeout(ctx, gp.bootstrapTimeout)
	defer bootstrapCancel()

	bootstrapCh := make(chan *GetPeersResponseMsg, len(gp.bootstrapNodeIDs))
	bootstrapWg := sync.WaitGroup{}

	for _, bsPeer := range gp.bootstrapNodeIDs {
		bootstrapWg.Add(1)
		go func(bsPeerID peer.ID) {
			defer bootstrapWg.Done()
			result := gp.queryBootstrapNode(bootstrapCtx, bsPeerID)
			if result != nil && result.Success {
				bootstrapCh <- result
			}
		}(bsPeer)
	}

	// Phase 2: Collect peers from PubSub in parallel
	pubsubCtx, pubsubCancel := context.WithTimeout(ctx, gp.timeout)
	defer pubsubCancel()

	pubsubCh := make(chan *GetPeerInfo, 100)
	pubsubDone := make(chan bool, 1)

	go func() {
		gp.collectPubSubPeers(pubsubCtx, pubsubCh)
		pubsubDone <- true
	}()

	// Wait for bootstrap queries to complete
	go func() {
		bootstrapWg.Wait()
		close(bootstrapCh)
	}()

	// Collect bootstrap results
	bootstrapCount := 0
	for result := range bootstrapCh {
		bootstrapCount++
		gp.mergePeers(result.Peers, "bootstrap")
		getpeersLog.Debugf(
			"[%s] GETPEERS: Bootstrap node returned %d peers",
			gp.requestID, len(result.Peers),
		)
	}

	// Collect PubSub results
	pubsubCount := 0
	go func() {
		for {
			select {
			case peerInfo := <-pubsubCh:
				if peerInfo != nil {
					pubsubCount++
					gp.mergePeers([]GetPeerInfo{*peerInfo}, "pubsub")
				}
			case <-pubsubDone:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for pubsub to finish or timeout
	select {
	case <-pubsubDone:
	case <-ctx.Done():
	}

	// Phase 3: Deduplicate and return results
	gp.mu.RLock()
	allPeers := make([]GetPeerInfo, 0, len(gp.discoveredPeers))
	for _, peerInfo := range gp.discoveredPeers {
		allPeers = append(allPeers, *peerInfo)
	}
	gp.mu.RUnlock()

	response.Peers = allPeers
	response.PeerCount = len(allPeers)
	response.LatencyMs = time.Since(startTime).Milliseconds()

	// Determine source type
	if bootstrapCount > 0 && pubsubCount > 0 {
		response.SourceType = "combined"
	} else if bootstrapCount > 0 {
		response.SourceType = "bootstrap"
	} else if pubsubCount > 0 {
		response.SourceType = "pubsub"
	} else {
		response.Error = "no peers discovered from any source"
		response.Success = false
		getpeersLog.Warnf("[%s] GETPEERS FAILED - no peers found", gp.requestID)
		response.LatencyMs = time.Since(startTime).Milliseconds()
		return response, fmt.Errorf("no peers discovered")
	}

	response.Success = true
	getpeersLog.Infof(
		"[%s] GETPEERS SUCCESS - subnet=%s, peers=%d (bootstrap=%d, pubsub=%d, source=%s), latency=%dms ✓",
		gp.requestID, gp.subnetID, response.PeerCount, bootstrapCount, pubsubCount, response.SourceType, response.LatencyMs,
	)

	return response, nil
}

// queryBootstrapNode queries a single bootstrap node for peers
func (gp *RDAGetPeers) queryBootstrapNode(ctx context.Context, bsPeerID peer.ID) *GetPeersResponseMsg {
	startTime := time.Now()

	getpeersLog.Debugf(
		"[%s] GETPEERS: Querying bootstrap node %s for subnet %s",
		gp.requestID, bsPeerID.String()[:8], gp.subnetID,
	)

	// Attempt to open stream to bootstrap node
	stream, err := gp.host.NewStream(ctx, bsPeerID, RDAGetPeersProtocol)
	if err != nil {
		getpeersLog.Warnf(
			"[%s] GETPEERS: Failed to connect to bootstrap node %s: %v",
			gp.requestID, bsPeerID.String()[:8], err,
		)
		return nil
	}
	defer stream.Close()

	// Send GETPEERS request
	req := GetPeersRequestMsg{
		RequestID: gp.requestID,
		NodeID:    gp.myPeerID.String(),
		SubnetID:  gp.subnetID,
		Timestamp: time.Now().UnixNano() / 1e6,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		getpeersLog.Warnf(
			"[%s] GETPEERS: Failed to marshal request: %v",
			gp.requestID, err,
		)
		return nil
	}

	_, err = stream.Write(reqData)
	if err != nil {
		getpeersLog.Warnf(
			"[%s] GETPEERS: Failed to send request to bootstrap node: %v",
			gp.requestID, err,
		)
		return nil
	}

	// Read response
	buf := make([]byte, 65536) // 64KB buffer
	n, err := stream.Read(buf)
	if err != nil {
		getpeersLog.Warnf(
			"[%s] GETPEERS: Failed to read response from bootstrap node: %v",
			gp.requestID, err,
		)
		return nil
	}

	// Parse response
	var resp GetPeersResponseMsg
	err = json.Unmarshal(buf[:n], &resp)
	if err != nil {
		getpeersLog.Warnf(
			"[%s] GETPEERS: Failed to parse bootstrap response: %v",
			gp.requestID, err,
		)
		return nil
	}

	latency := time.Since(startTime).Milliseconds()
	getpeersLog.Debugf(
		"[%s] GETPEERS: Bootstrap response received - %d peers, latency=%dms",
		gp.requestID, len(resp.Peers), latency,
	)

	return &resp
}

// collectPubSubPeers nghe trên PubSub channel để thu thập peers
func (gp *RDAGetPeers) collectPubSubPeers(ctx context.Context, outCh chan *GetPeerInfo) {
	defer close(outCh)

	getpeersLog.Debugf(
		"[%s] GETPEERS: Listening on PubSub topic: %s",
		gp.requestID, gp.subnetID,
	)

	// Join the topic
	topic, err := gp.pubsub.Join(gp.subnetID)
	if err != nil {
		getpeersLog.Warnf(
			"[%s] GETPEERS: Failed to join topic %s: %v",
			gp.requestID, gp.subnetID, err,
		)
		return
	}
	defer topic.Close()

	// Subscribe to topic
	sub, err := topic.Subscribe()
	if err != nil {
		getpeersLog.Warnf(
			"[%s] GETPEERS: Failed to subscribe to topic %s: %v",
			gp.requestID, gp.subnetID, err,
		)
		return
	}
	defer sub.Cancel()

	// Get all current peers on the topic
	currentPeers := topic.ListPeers()
	getpeersLog.Debugf(
		"[%s] GETPEERS: Found %d peers on PubSub topic %s",
		gp.requestID, len(currentPeers), gp.subnetID,
	)

	for _, peerID := range currentPeers {
		if peerID != gp.myPeerID {
			peerInfo := &GetPeerInfo{
				PeerID:    peerID.String(),
				Addresses: getPeerAddresses(gp.host, peerID),
				LastSeen:  time.Now().UnixNano() / 1e6,
				Source:    "pubsub",
			}
			select {
			case outCh <- peerInfo:
			case <-ctx.Done():
				return
			}
		}
	}

	// Listen for new messages/peer joins in the topic
	// This gives us a continuous stream of active peers
	msgCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := sub.Next(msgCtx)
			if err != nil {
				return // Timeout or context done
			}

			// Record the peer that sent this message
			// msg.From is []byte, need to decode it to peer.ID
			fromPeerID, err := peer.Decode(string(msg.From))
			if err != nil {
				continue // Skip if cannot decode peer ID
			}
			if fromPeerID != gp.myPeerID {
				peerInfo := &GetPeerInfo{
					PeerID:    fromPeerID.String(),
					Addresses: getPeerAddresses(gp.host, fromPeerID),
					LastSeen:  time.Now().UnixNano() / 1e6,
					Source:    "pubsub",
				}
				select {
				case outCh <- peerInfo:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// mergePeers merges discovered peers and deduplicates
func (gp *RDAGetPeers) mergePeers(peers []GetPeerInfo, source string) {
	gp.mu.Lock()
	defer gp.mu.Unlock()

	for _, peerInfo := range peers {
		peerID := peerInfo.PeerID

		if existing, ok := gp.discoveredPeers[peerID]; ok {
			// Update last seen if this is more recent
			if peerInfo.LastSeen > existing.LastSeen {
				existing.LastSeen = peerInfo.LastSeen
			}
			// Keep both sources recorded
			if source == "bootstrap" && existing.Source == "pubsub" {
				existing.Source = "combined"
			} else if source == "pubsub" && existing.Source == "bootstrap" {
				existing.Source = "combined"
			}
		} else {
			// New peer - add it
			newPeerInfo := peerInfo
			newPeerInfo.Source = source
			gp.discoveredPeers[peerID] = &newPeerInfo

			// Track by source
			if source == "bootstrap" {
				gp.bootstrapPeerMap[peerID] = &newPeerInfo
			} else if source == "pubsub" {
				gp.pubsubPeers[peerID] = &newPeerInfo
			}

			getpeersLog.Debugf(
				"[%s] GETPEERS: Discovered peer %s from %s",
				gp.requestID, peerID[:8], source,
			)
		}
	}
}

// GetDiscoveredPeers returns current discovered peers (for debugging)
func (gp *RDAGetPeers) GetDiscoveredPeers() []GetPeerInfo {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

	peers := make([]GetPeerInfo, 0, len(gp.discoveredPeers))
	for _, peerInfo := range gp.discoveredPeers {
		peers = append(peers, *peerInfo)
	}
	return peers
}

// GetDiscoveryStats returns statistics about discovery
func (gp *RDAGetPeers) GetDiscoveryStats() map[string]interface{} {
	gp.mu.RLock()
	defer gp.mu.RUnlock()

	return map[string]interface{}{
		"request_id":      gp.requestID,
		"subnet_id":       gp.subnetID,
		"total_peers":     len(gp.discoveredPeers),
		"bootstrap_peers": len(gp.bootstrapPeerMap),
		"pubsub_peers":    len(gp.pubsubPeers),
		"combined_peers":  len(gp.discoveredPeers) - len(gp.bootstrapPeerMap) - len(gp.pubsubPeers),
	}
}

// Helper to get peer addresses from host's peerstore
func getPeerAddresses(h host.Host, peerID peer.ID) []string {
	addrs := h.Peerstore().Addrs(peerID)
	addrStrs := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		addrStrs = append(addrStrs, addr.String())
	}
	return addrStrs
}
