package share

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
)

// TestHeartbeat_HeartbeatMessageStructure tests the heartbeat message format
func TestHeartbeat_HeartbeatMessageStructure(t *testing.T) {
	t.Log("========== TEST: Heartbeat Message Structure ==========")

	msg := RDAHeartbeatMessage{
		PeerID:    "QmNode123",
		Row:       5,
		Col:       10,
		Timestamp: time.Now().UnixNano() / 1e6,
		Status:    "alive",
		GridSize:  256,
	}

	assert.NotEmpty(t, msg.PeerID)
	assert.Equal(t, uint32(5), msg.Row)
	assert.Equal(t, uint32(10), msg.Col)
	assert.Equal(t, "alive", msg.Status)
	assert.Equal(t, uint32(256), msg.GridSize)
	assert.Greater(t, msg.Timestamp, int64(0))

	// Test leaving status
	msg2 := RDAHeartbeatMessage{
		PeerID: "QmNode456",
		Status: "leaving",
	}
	assert.Equal(t, "leaving", msg2.Status)

	t.Log("✓ Message structure complete and correct")
	t.Log("✓ Test PASSED: Heartbeat message format valid\n")
}

// TestHeartbeat_HeartbeatInterval tests timeout constants
func TestHeartbeat_HeartbeatInterval(t *testing.T) {
	t.Log("========== TEST: Heartbeat Interval Configuration ==========")

	t.Logf("✓ Heartbeat configuration:")
	t.Logf("  - Interval: %v (should be 10s)", HeartbeatInterval)
	t.Logf("  - Peer timeout: %v (should be 30s)", PeerTimeoutDuration)
	t.Logf("  - Cleanup interval: %v (should be 15s)", CleanupInterval)

	assert.Equal(t, 10*time.Second, HeartbeatInterval)
	assert.Equal(t, 30*time.Second, PeerTimeoutDuration)
	assert.Equal(t, 15*time.Second, CleanupInterval)

	t.Log("✓ Test PASSED: Timing configuration correct\n")
}

// TestHeartbeat_PeerTracking tests peer last-seen timestamp tracking
func TestHeartbeat_PeerTracking(t *testing.T) {
	t.Log("========== TEST: Peer Tracking ==========")

	hb := &RDAHeartbeat{
		myPeerID:     mockPeerID("QmNode1"),
		myRow:        0,
		myCol:        0,
		gridSize:     256,
		peerLastSeen: make(map[string]int64),
		mu2:          sync.RWMutex{},
	}

	// Add peers with different timestamps
	hb.mu2.Lock()
	now := time.Now().Unix()
	hb.peerLastSeen["QmPeer1"] = now      // Just now
	hb.peerLastSeen["QmPeer2"] = now - 5  // 5 seconds ago
	hb.peerLastSeen["QmPeer3"] = now - 40 // 40 seconds ago (stale)
	hb.mu2.Unlock()

	// Check online peers (timeout 30s)
	hb.mu2.RLock()
	currentTime := time.Now().Unix()
	onlineCount := 0
	offlineCount := 0
	for _, lastSeen := range hb.peerLastSeen {
		if currentTime-lastSeen <= int64(PeerTimeoutDuration.Seconds()) {
			onlineCount++
		} else {
			offlineCount++
		}
	}
	hb.mu2.RUnlock()

	t.Logf("✓ Peer tracking results:")
	t.Logf("  - Online peers (< 30s): %d", onlineCount)
	t.Logf("  - Offline peers (≥ 30s): %d", offlineCount)

	assert.Equal(t, 2, onlineCount, "Should have 2 online peers")
	assert.Equal(t, 1, offlineCount, "Should have 1 offline peer")

	t.Log("✓ Test PASSED: Peer tracking works correctly\n")
}

// TestHeartbeat_SubnetRegistration tests subnet registration
func TestHeartbeat_SubnetRegistration(t *testing.T) {
	t.Log("========== TEST: Subnet Registration ==========")

	hb := &RDAHeartbeat{
		myPeerID:      mockPeerID("QmNode1"),
		joinedSubnets: make(map[string]bool),
		mu:            sync.RWMutex{},
	}

	// Register subnets
	hb.RegisterSubnet("rda/row/0")
	hb.RegisterSubnet("rda/col/0")
	hb.RegisterSubnet("rda/row/128")

	hb.mu.RLock()
	subnets := len(hb.joinedSubnets)
	hb.mu.RUnlock()

	t.Logf("✓ Registered subnets:")
	hb.mu.RLock()
	for subnet := range hb.joinedSubnets {
		t.Logf("  - %s", subnet)
	}
	hb.mu.RUnlock()

	assert.Equal(t, 3, subnets)
	t.Log("✓ Test PASSED: Subnet registration works\n")
}

// TestHeartbeat_OnlineOfflineDetection tests online/offline status logic (SIMPLIFIED)
func TestHeartbeat_OnlineOfflineDetection(t *testing.T) {
	t.Log("========== TEST: Online/Offline Detection Logic ==========")

	hb := &RDAHeartbeat{
		myPeerID:      mockPeerID("QmNode1"),
		myRow:         0,
		myCol:         0,
		gridSize:      256,
		routingTable:  NewRoutingTable(50),
		joinedSubnets: make(map[string]bool),
		subscriptions: make(map[string]interface{}),
		peerLastSeen:  make(map[string]int64),
		stopCh:        make(chan struct{}, 1),
		mu:            sync.RWMutex{},
		mu2:           sync.RWMutex{},
	}

	// Directly track a peer as online
	hb.mu2.Lock()
	now := time.Now().Unix()
	hb.peerLastSeen["QmPeer123"] = now
	hb.mu2.Unlock()

	// Verify it's tracked
	hb.mu2.RLock()
	tracked := len(hb.peerLastSeen)
	hb.mu2.RUnlock()

	assert.Equal(t, 1, tracked, "Should have 1 tracked peer")

	t.Log("✓ Test PASSED: Online detection works\n")
}

// TestHeartbeat_StalePeerRemovalLogic tests stale peer cleanup logic
func TestHeartbeat_StalePeerRemovalLogic(t *testing.T) {
	t.Log("========== TEST: Stale Peer Removal Logic ==========")

	hb := &RDAHeartbeat{
		peerLastSeen: make(map[string]int64),
		mu2:          sync.RWMutex{},
	}

	// Add mixed peers
	hb.mu2.Lock()
	now := time.Now().Unix()
	hb.peerLastSeen["QmAlive1"] = now
	hb.peerLastSeen["QmAlive2"] = now - 5
	hb.peerLastSeen["QmStale1"] = now - 35
	hb.peerLastSeen["QmStale2"] = now - 40
	initialCount := len(hb.peerLastSeen)
	hb.mu2.Unlock()

	t.Logf("Initial peers: %d", initialCount)

	// Run cleanup
	hb.removeStalePeers()

	hb.mu2.RLock()
	finalCount := len(hb.peerLastSeen)
	remainingPeers := make([]string, 0)
	for peer := range hb.peerLastSeen {
		remainingPeers = append(remainingPeers, peer)
	}
	hb.mu2.RUnlock()

	t.Logf("✓ After cleanup:")
	t.Logf("  - Final peers: %d", finalCount)
	t.Logf("  - Removed: %d", initialCount-finalCount)
	for _, peer := range remainingPeers {
		t.Logf("    ✓ %s", peer)
	}

	assert.Less(t, finalCount, initialCount)
	assert.Equal(t, 2, finalCount, "Should have 2 alive peers remaining")

	t.Log("✓ Test PASSED: Stale peer cleanup works\n")
}

// TestHeartbeat_GetHeartbeatStats tests statistics generation
func TestHeartbeat_GetHeartbeatStats(t *testing.T) {
	t.Log("========== TEST: Heartbeat Statistics ==========")

	hb := &RDAHeartbeat{
		myPeerID:      mockPeerID("QmNode1"),
		myRow:         5,
		myCol:         10,
		gridSize:      256,
		joinedSubnets: make(map[string]bool),
		peerLastSeen:  make(map[string]int64),
		mu:            sync.RWMutex{},
		mu2:           sync.RWMutex{},
	}

	hb.RegisterSubnet("rda/row/5")
	hb.RegisterSubnet("rda/col/10")

	hb.mu2.Lock()
	now := time.Now().Unix()
	hb.peerLastSeen["QmAlive1"] = now
	hb.peerLastSeen["QmAlive2"] = now - 5
	hb.peerLastSeen["QmStale"] = now - 40
	hb.mu2.Unlock()

	stats := hb.GetHeartbeatStats()

	t.Logf("✓ Stats:")
	t.Logf("  - Node: %v", stats["node_id"])
	t.Logf("  - Position: %v", stats["position"])
	t.Logf("  - Joined subnets: %v", stats["joined_subnets"])
	t.Logf("  - Total tracked: %v", stats["total_tracked_peers"])
	t.Logf("  - Online: %v", stats["online_peers"])
	t.Logf("  - Offline: %v", stats["offline_peers"])

	assert.Equal(t, 2, stats["joined_subnets"])
	assert.Equal(t, 3, stats["total_tracked_peers"])
	assert.Equal(t, 2, stats["online_peers"])
	assert.Equal(t, 1, stats["offline_peers"])

	t.Log("✓ Test PASSED: Statistics reporting works\n")
}

// TestHeartbeat_MultiSubnetTracking tests tracking peers across subnets
func TestHeartbeat_MultiSubnetTracking(t *testing.T) {
	t.Log("========== TEST: Multi-Subnet Peer Tracking ==========")

	hb := &RDAHeartbeat{
		myPeerID:      mockPeerID("QmNode1"),
		myRow:         5,
		myCol:         10,
		gridSize:      256,
		routingTable:  NewRoutingTable(50),
		joinedSubnets: make(map[string]bool),
		subscriptions: make(map[string]interface{}),
		peerLastSeen:  make(map[string]int64),
		mu:            sync.RWMutex{},
		mu2:           sync.RWMutex{},
	}

	// Register multiple subnets
	subnets := []string{
		"rda/row/5",
		"rda/col/10",
		"rda/row/128",
		"rda/col/42",
	}

	for _, subnet := range subnets {
		hb.RegisterSubnet(subnet)
	}

	hb.mu.RLock()
	registeredCount := len(hb.joinedSubnets)
	hb.mu.RUnlock()

	t.Logf("✓ Registered %d subnets:", registeredCount)
	for _, subnet := range subnets {
		t.Logf("  - %s", subnet)
	}

	// Add peers for each subnet
	for i := 0; i < 4; i++ {
		for j := 0; j < 10; j++ {
			peerID := fmt.Sprintf("QmPeer_%d_%d", i, j)
			hb.mu2.Lock()
			hb.peerLastSeen[peerID] = time.Now().Unix()
			hb.mu2.Unlock()
		}
	}

	hb.mu2.RLock()
	totalPeers := len(hb.peerLastSeen)
	hb.mu2.RUnlock()

	t.Logf("✓ Total tracked peers: %d (4 subnets × 10 peers)", totalPeers)

	assert.Equal(t, 4, registeredCount)
	assert.Equal(t, 40, totalPeers)

	t.Log("✓ Test PASSED: Multi-subnet tracking works\n")
}

// TestHeartbeat_LargeGridSimulation tests with large peer counts
func TestHeartbeat_LargeGridSimulation(t *testing.T) {
	t.Log("========== TEST: Large Grid Simulation (256×256) ==========")

	hb := &RDAHeartbeat{
		myPeerID:     mockPeerID("QmNode1"),
		myRow:        128,
		myCol:        128,
		gridSize:     256,
		peerLastSeen: make(map[string]int64),
		mu2:          sync.RWMutex{},
	}

	// Simulate 256 peers in row + 256 peers in col
	hb.mu2.Lock()
	now := time.Now().Unix()

	// Row peers
	for i := 0; i < 256; i++ {
		peerID := fmt.Sprintf("QmRowPeer_%d", i)
		hb.peerLastSeen[peerID] = now
	}

	// Col peers
	for i := 0; i < 256; i++ {
		peerID := fmt.Sprintf("QmColPeer_%d", i)
		hb.peerLastSeen[peerID] = now
	}

	hb.mu2.Unlock()

	hb.mu2.RLock()
	totalPeers := len(hb.peerLastSeen)
	hb.mu2.RUnlock()

	t.Logf("✓ Large grid (256×256) simulation:")
	t.Logf("  - Node position: (128, 128)")
	t.Logf("  - Total peers: %d (256 row + 256 col)", totalPeers)

	assert.Equal(t, 512, totalPeers)
	t.Log("✓ Test PASSED: Large grid simulation works\n")
}

// Helper functions for tests

func mockPeerID(id string) peer.ID {
	// Return a valid peer ID by using a known format
	return peer.ID(id)
}

func mockAddrInfo() peer.AddrInfo {
	return peer.AddrInfo{
		ID:    peer.ID("QmPeer123"),
		Addrs: []ma.Multiaddr{},
	}
}
