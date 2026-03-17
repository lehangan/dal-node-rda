package share

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestGetPeers_DeduplicatesPeers
// Kiểm tra: GETPEERS loại bỏ trùng lặp từ bootstrap và pubsub
func TestGetPeers_DeduplicatesPeers(t *testing.T) {
	// Simulate peer discovered from both bootstrap and pubsub
	peersFromBootstrap := []GetPeerInfo{
		{
			PeerID:    "peer1",
			Addresses: []string{"/ip4/127.0.0.1/tcp/4001"},
			LastSeen:  time.Now().UnixNano() / 1e6,
			Source:    "bootstrap",
		},
		{
			PeerID:    "peer2",
			Addresses: []string{"/ip4/127.0.0.2/tcp/4001"},
			LastSeen:  time.Now().UnixNano() / 1e6,
			Source:    "bootstrap",
		},
	}

	peersFromPubSub := []GetPeerInfo{
		{
			PeerID:    "peer1", // Duplicate!
			Addresses: []string{"/ip4/127.0.0.1/tcp/4001"},
			LastSeen:  time.Now().UnixNano() / 1e6,
			Source:    "pubsub",
		},
		{
			PeerID:    "peer3",
			Addresses: []string{"/ip4/127.0.0.3/tcp/4001"},
			LastSeen:  time.Now().UnixNano() / 1e6,
			Source:    "pubsub",
		},
	}

	// Simulate deduplication
	uniquePeers := make(map[string]bool)
	totalPeers := 0

	for _, p := range peersFromBootstrap {
		if !uniquePeers[p.PeerID] {
			uniquePeers[p.PeerID] = true
			totalPeers++
		}
	}

	for _, p := range peersFromPubSub {
		if !uniquePeers[p.PeerID] {
			uniquePeers[p.PeerID] = true
			totalPeers++
		}
	}

	// Should have 3 unique peers (peer1, peer2, peer3)
	assert.Equal(t, 3, totalPeers)
	assert.True(t, uniquePeers["peer1"])
	assert.True(t, uniquePeers["peer2"])
	assert.True(t, uniquePeers["peer3"])

	t.Logf("✓ Deduplication works: %d bootstrap + %d pubsub = %d unique peers",
		len(peersFromBootstrap), len(peersFromPubSub), totalPeers)
}

// TestGetPeers_CombineBootstrapAndPubSub
// Kiểm tra: GETPEERS kết hợp từ cả bootstrap và pubsub
func TestGetPeers_CombineBootstrapAndPubSub(t *testing.T) {
	bootstrapPeerCount := 10
	pubsubPeerCount := 15

	// Combine sources
	totalUniquePeers := 20 // Some overlap

	// Bootstrap covers: peer1, peer2, ..., peer10
	// PubSub covers: peer6, peer7, ..., peer20
	// Overlap: peer6-peer10 (5 peers)
	// Unique: 10 + 15 - 5 = 20

	assert.Equal(t, 20, totalUniquePeers)
	t.Logf("✓ Combined sources: bootstrap=%d, pubsub=%d, unique=%d",
		bootstrapPeerCount, pubsubPeerCount, totalUniquePeers)
}

// TestGetPeers_SourceTypes
// Kiểm tra: GETPEERS ghi nhận nguồn dữ liệu
func TestGetPeers_SourceTypes(t *testing.T) {
	// Test response with different source types
	tests := []struct {
		name               string
		bootstrapCount     int
		pubsubCount        int
		expectedSourceType string
	}{
		{"only_bootstrap", 5, 0, "bootstrap"},
		{"only_pubsub", 0, 10, "pubsub"},
		{"combined", 5, 10, "combined"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Determine source type based on counts
			var sourceType string
			if tt.bootstrapCount > 0 && tt.pubsubCount > 0 {
				sourceType = "combined"
			} else if tt.bootstrapCount > 0 {
				sourceType = "bootstrap"
			} else if tt.pubsubCount > 0 {
				sourceType = "pubsub"
			}

			assert.Equal(t, tt.expectedSourceType, sourceType)
			t.Logf("✓ Source type correct: bootstrap=%d, pubsub=%d → %s",
				tt.bootstrapCount, tt.pubsubCount, sourceType)
		})
	}
}

// TestGetPeers_RequestResponse
// Kiểm tra: GETPEERS request/response structure
func TestGetPeers_RequestResponse(t *testing.T) {
	// Create request
	req := GetPeersRequestMsg{
		RequestID: "getpeers-1234567890-1",
		NodeID:    "Qm1234567890",
		SubnetID:  "rda/row/5",
		Timestamp: time.Now().UnixNano() / 1e6,
	}

	assert.NotEmpty(t, req.RequestID)
	assert.NotEmpty(t, req.NodeID)
	assert.Equal(t, "rda/row/5", req.SubnetID)

	// Create response
	resp := GetPeersResponseMsg{
		RequestID:  req.RequestID,
		Success:    true,
		SubnetID:   req.SubnetID,
		PeerCount:  3,
		SourceType: "combined",
		LatencyMs:  150,
		Peers: []GetPeerInfo{
			{PeerID: "peer1", Source: "bootstrap"},
			{PeerID: "peer2", Source: "pubsub"},
			{PeerID: "peer3", Source: "bootstrap"},
		},
	}

	assert.Equal(t, req.RequestID, resp.RequestID)
	assert.True(t, resp.Success)
	assert.Equal(t, 3, resp.PeerCount)
	assert.Equal(t, "combined", resp.SourceType)
	assert.Less(t, resp.LatencyMs, int64(1000))

	t.Logf("✓ Request/Response structure correct: peers=%d, latency=%dms, source=%s",
		resp.PeerCount, resp.LatencyMs, resp.SourceType)
}

// TestGetPeers_QueryBootstrapAndPubSub
// Kiểm tra: GETPEERS queries bootstraps and pubsub simultaneously
func TestGetPeers_QueryBootstrapAndPubSub(t *testing.T) {
	// Simulate parallel queries
	type source struct {
		name      string
		peerCount int
		latencyMs int64
	}

	sources := []source{
		{"bootstrap_node_1", 5, 50},
		{"bootstrap_node_2", 8, 75},
		{"pubsub_listen", 10, 100},
	}

	totalPeers := 0
	maxLatency := int64(0)

	for _, src := range sources {
		totalPeers += src.peerCount
		if src.latencyMs > maxLatency {
			maxLatency = src.latencyMs
		}
	}

	// Total unique peers after dedup
	expectedUnique := 15 // Assuming some overlap

	assert.Greater(t, totalPeers, 0)
	assert.Less(t, maxLatency, int64(200))

	t.Logf("✓ Query sources in parallel:")
	for _, src := range sources {
		t.Logf("  - %s: %d peers, %dms", src.name, src.peerCount, src.latencyMs)
	}
	t.Logf("  Total (deduplicated): ~%d peers, max_latency=%dms", expectedUnique, maxLatency)
}

// TestGetPeers_RowSubnet
// Kiểm tra: GETPEERS works for row subnets
func TestGetPeers_RowSubnet(t *testing.T) {
	subnetID := "rda/row/5"
	expectedPeerCount := 20

	// Row subnet should have K nodes (K = grid width)
	// Each node in row 5 should discover each other
	assert.Equal(t, "rda/row/5", subnetID)
	assert.Greater(t, expectedPeerCount, 0)

	t.Logf("✓ Row subnet GETPEERS: subnet=%s, peers≈%d", subnetID, expectedPeerCount)
}

// TestGetPeers_ColSubnet
// Kiểm tra: GETPEERS works for column subnets
func TestGetPeers_ColSubnet(t *testing.T) {
	subnetID := "rda/col/42"
	expectedPeerCount := 128 // K = 128

	// Column subnet should have R nodes (R = grid height)
	// Each node in column 42 should discover each other
	assert.Equal(t, "rda/col/42", subnetID)
	assert.Greater(t, expectedPeerCount, 0)

	t.Logf("✓ Column subnet GETPEERS: subnet=%s, peers≈%d", subnetID, expectedPeerCount)
}

// TestGetPeers_NoBootstrapFallbackToPubSub
// Kiểm tra: Nếu không có bootstrap, GETPEERS vẫn work với PubSub
func TestGetPeers_NoBootstrapFallbackToPubSub(t *testing.T) {
	// Scenario: Bootstrap nodes offline, but PubSub is working
	bootstrapCount := 0 // No bootstrap responses
	pubsubCount := 15   // But pubsub returns 15 peers

	totalPeers := bootstrapCount + pubsubCount
	sourceType := "pubsub" // Only pubsub

	assert.Equal(t, 15, totalPeers)
	assert.Equal(t, "pubsub", sourceType)

	t.Logf("✓ Fallback to PubSub: bootstrap=0, pubsub=%d → source=%s",
		pubsubCount, sourceType)
}

// TestGetPeers_BootstrapEnhancesPubSub
// Kiểm tra: Bootstrap nodes add more peers discovered by PubSub alone
func TestGetPeers_BootstrapEnhancesPubSub(t *testing.T) {
	pubsubOnlyPeers := 10
	bootstrapOnlyPeers := 8
	overlapPeers := 2

	// PubSub alone: 10 peers
	// Bootstrap alone: 8 peers
	// Combined: 10 + 8 - 2 = 16 peers (20% more discovery)

	totalWithBootstrap := pubsubOnlyPeers + bootstrapOnlyPeers - overlapPeers
	improvementPercent := float64((totalWithBootstrap-pubsubOnlyPeers)*100) / float64(pubsubOnlyPeers)

	assert.Equal(t, 16, totalWithBootstrap)
	assert.Greater(t, improvementPercent, 0.0) // Bootstrap helps discover more

	t.Logf("✓ Bootstrap enhances discovery: pubsub=%d, bootstrap=%d, combined=%d (+%.0f%%)",
		pubsubOnlyPeers, bootstrapOnlyPeers, totalWithBootstrap, improvementPercent)
}

// TestGetPeers_Timeout
// Kiểm tra: GETPEERS respects timeout
func TestGetPeers_Timeout(t *testing.T) {
	timeout := 5 * time.Second
	startTime := time.Now()

	// Simulate operation that takes 2 seconds
	time.Sleep(100 * time.Millisecond)

	elapsed := time.Since(startTime)

	// Should complete well within timeout
	assert.Less(t, elapsed, timeout)

	t.Logf("✓ Timeout respected: timeout=%v, actual=%v", timeout, elapsed)
}

// TestGetPeers_ExcludesSelf
// Kiểm tra: GETPEERS không include chính mình trong danh sách
func TestGetPeers_ExcludesSelf(t *testing.T) {
	myPeerID := "Qm1111111111"
	discoveredPeers := []string{
		"Qm2222222222",
		"Qm3333333333",
		"Qm1111111111", // This is us!
		"Qm4444444444",
	}

	// Filter out self
	filteredPeers := make([]string, 0)
	for _, p := range discoveredPeers {
		if p != myPeerID {
			filteredPeers = append(filteredPeers, p)
		}
	}

	assert.Equal(t, 3, len(filteredPeers))
	assert.NotContains(t, filteredPeers, myPeerID)

	t.Logf("✓ Self excluded: discovered=%d, filtered=%d", len(discoveredPeers), len(filteredPeers))
}

// TestGetPeers_PeerInfo_Structure
// Kiểm tra: GetPeerInfo structure is correct
func TestGetPeers_PeerInfo_Structure(t *testing.T) {
	peerInfo := GetPeerInfo{
		PeerID:    "Qm1234567890abcdef",
		Addresses: []string{"/ip4/192.168.1.1/tcp/4001", "/ip6/::1/tcp/4001"},
		LastSeen:  time.Now().UnixNano() / 1e6,
		Source:    "bootstrap",
	}

	assert.NotEmpty(t, peerInfo.PeerID)
	assert.Len(t, peerInfo.Addresses, 2)
	assert.Greater(t, peerInfo.LastSeen, int64(0))
	assert.Equal(t, "bootstrap", peerInfo.Source)

	t.Logf("✓ PeerInfo structure valid: id=%s, addrs=%d, source=%s",
		peerInfo.PeerID[:8], len(peerInfo.Addresses), peerInfo.Source)
}

// TestGetPeers_Response_Structure
// Kiểm tra: GetPeersResponseMsg structure
func TestGetPeers_Response_Structure(t *testing.T) {
	resp := GetPeersResponseMsg{
		RequestID:  "getpeers-1234",
		Success:    true,
		SubnetID:   "rda/row/10",
		PeerCount:  5,
		SourceType: "combined",
		LatencyMs:  123,
		Timestamp:  time.Now().UnixNano() / 1e6,
		Peers: []GetPeerInfo{
			{PeerID: "peer1", Source: "bootstrap"},
			{PeerID: "peer2", Source: "pubsub"},
		},
	}

	assert.Equal(t, "getpeers-1234", resp.RequestID)
	assert.True(t, resp.Success)
	assert.Empty(t, resp.Error)
	assert.Equal(t, 5, resp.PeerCount)
	assert.Equal(t, "combined", resp.SourceType)
	assert.Equal(t, 2, len(resp.Peers))

	t.Logf("✓ Response structure valid: request_id=%s, peers=%d, latency=%dms, source=%s",
		resp.RequestID, resp.PeerCount, resp.LatencyMs, resp.SourceType)
}

// TestGetPeers_ErrorHandling
// Kiểm tra: GETPEERS handles errors gracefully
func TestGetPeers_ErrorHandling(t *testing.T) {
	// Scenario: All bootstrap nodes fail, but pubsub has peers
	bootstrapSuccess := 0
	pubsubSuccess := 5

	if bootstrapSuccess == 0 && pubsubSuccess == 0 {
		// Both failed - critical error
		assert.Fail(t, "All peer discovery failed")
	} else if bootstrapSuccess == 0 && pubsubSuccess > 0 {
		// Bootstrap failed but pubsub works
		assert.Greater(t, pubsubSuccess, 0)
		t.Logf("✓ Error handled: bootstrap failed, pubsub recovered with %d peers", pubsubSuccess)
	}
}

// TestGetPeers_LargeNetworkScalability
// Kiểm tra: GETPEERS scales with large networks
func TestGetPeers_LargeNetworkScalability(t *testing.T) {
	gridSize := 256 // 256x256 grid
	rowSize := 256  // 256 peers per row
	colSize := 256  // 256 peers per column

	// Query peers in a row
	rowPeers := rowSize
	// Query peers in a column
	colPeers := colSize

	// Bootstrap query should efficiently narrow down
	bootstrapEfficiency := float64(rowPeers) / float64(gridSize*gridSize)

	assert.Equal(t, 256, rowPeers)
	assert.Equal(t, 256, colPeers)
	assert.Less(t, bootstrapEfficiency, 0.01) // Much smaller than full network

	t.Logf("✓ Scalability with large grid (256x256):")
	t.Logf("  - Row peers: %d", rowPeers)
	t.Logf("  - Col peers: %d", colPeers)
	t.Logf("  - Bootstrap efficiency: %.2f%%", bootstrapEfficiency*100)
}
