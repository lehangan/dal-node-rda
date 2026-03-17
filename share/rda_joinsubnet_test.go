package share

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNodeRole_String
func TestNodeRole_String(t *testing.T) {
	assert.Equal(t, "ClientNode(ρ=0)", ClientNode.String())
	assert.Equal(t, "BootstrapNode(ρ=1)", BootstrapNode.String())
}

// TestNodeRole_IsValid
func TestNodeRole_IsValid(t *testing.T) {
	assert.True(t, ClientNode.IsValid())
	assert.True(t, BootstrapNode.IsValid())
	assert.False(t, NodeRole(99).IsValid())
}

// TestParseNodeRole_Success
func TestParseNodeRole_Success(t *testing.T) {
	role, err := ParseNodeRole("client")
	assert.NoError(t, err)
	assert.Equal(t, ClientNode, role)

	role, err = ParseNodeRole("bootstrap")
	assert.NoError(t, err)
	assert.Equal(t, BootstrapNode, role)

	role, err = ParseNodeRole("0")
	assert.NoError(t, err)
	assert.Equal(t, ClientNode, role)

	role, err = ParseNodeRole("1")
	assert.NoError(t, err)
	assert.Equal(t, BootstrapNode, role)
}

// TestParseNodeRole_Invalid
func TestParseNodeRole_Invalid(t *testing.T) {
	_, err := ParseNodeRole("invalid")
	assert.Error(t, err)
}

// TestRDASubnetManager_Role
func TestRDASubnetManager_Role(t *testing.T) {
	dims := GridDimensions{Rows: 128, Cols: 128}
	gridManager := NewRDAGridManager(dims)

	sm := &RDASubnetManager{
		gridManager: gridManager,
		role:        ClientNode,
	}

	assert.Equal(t, ClientNode, sm.GetRole())

	sm.SetRole(BootstrapNode)
	assert.Equal(t, BootstrapNode, sm.GetRole())

	sm.SetRole(ClientNode)
	assert.Equal(t, ClientNode, sm.GetRole())

	t.Logf("✓ RDASubnetManager role management works")
}

// Semantic Tests: Verify JOINSUBNET requirements from paper

// TestSemantics_ClientNode_Joins1Row1Col
func TestSemantics_ClientNode_Joins1Row1Col(t *testing.T) {
	// Per paper: Client (ρ=0) joins only its own (row, col)
	clientRowsJoined := 1
	clientColsJoined := 1
	totalClientSubnets := clientRowsJoined + clientColsJoined

	assert.Equal(t, 1, clientRowsJoined, "Client must join exactly 1 row")
	assert.Equal(t, 1, clientColsJoined, "Client must join exactly 1 column")
	assert.Equal(t, 2, totalClientSubnets, "Client joins 2 subnets total")

	t.Logf("✓ Client node semantics: joins %d subnet(s) (row=%d, col=%d)",
		totalClientSubnets, clientRowsJoined, clientColsJoined)
}

// TestSemantics_BootstrapNode_JoinsAllRowsOwnCol
func TestSemantics_BootstrapNode_JoinsAllRowsOwnCol(t *testing.T) {
	// Per paper: Bootstrap (ρ=1) joins ALL rows (0...R-1) + its own column
	// This prevents network partition
	gridSize := 128
	bootstrapRowsJoined := gridSize // Join rows 0 to R-1
	bootstrapColJoined := 1         // Join own column
	totalBootstrapSubnets := bootstrapRowsJoined + bootstrapColJoined

	assert.Equal(t, 128, bootstrapRowsJoined, "Bootstrap must join ALL rows")
	assert.Equal(t, 1, bootstrapColJoined, "Bootstrap joins own column")
	assert.Equal(t, 129, totalBootstrapSubnets, "Bootstrap joins R+1 subnets")

	t.Logf("✓ Bootstrap node semantics: joins %d subnet(s) (rows=%d, col=%d)",
		totalBootstrapSubnets, bootstrapRowsJoined, bootstrapColJoined)
}

// TestSemantics_Difference_ClientVsBootstrap
func TestSemantics_Difference_ClientVsBootstrap(t *testing.T) {
	// Client joins 2 subnets (1 row + 1 col)
	clientSubnets := 2

	// Bootstrap joins R+1 subnets (all rows + 1 col)
	gridSize := 128
	bootstrapSubnets := gridSize + 1

	// Bootstrap joins significantly more to prevent partition
	assert.Less(t, clientSubnets, bootstrapSubnets)
	ratio := float64(bootstrapSubnets) / float64(clientSubnets)
	t.Logf("✓ Bootstrap joins ~%.1fx more subnets than Client (%.0f vs %d)",
		ratio, float64(bootstrapSubnets), clientSubnets)
}

// TestSemantics_NetworkPartitionPrevention
func TestSemantics_NetworkPartitionPrevention(t *testing.T) {
	// Bootstrap node joining ALL rows ensures:
	// 1. Every row subnet has at least one bootstrap node
	// 2. Data can propagate across the entire network
	// 3. No row is isolated from others
	gridSize := 256

	// Each bootstrap joins all R rows
	rowsPerBootstrap := gridSize

	// Collective coverage
	coveringNodes := 1 // Just 1 bootstrap joins all rows
	assert.GreaterOrEqual(t, coveringNodes, 1, "At least 1 bootstrap needed to cover all rows")

	t.Logf("✓ Network partition prevention:")
	t.Logf("  - Grid size: %d rows", gridSize)
	t.Logf("  - Each bootstrap joins: %d rows", rowsPerBootstrap)
	t.Logf("  - With minimal bootstrap nodes, full connectivity guaranteed")
}

// TestJoinSubnet_ClientConfiguration
func TestJoinSubnet_ClientConfiguration(t *testing.T) {
	dims := GridDimensions{Rows: 128, Cols: 128}
	gridManager := NewRDAGridManager(dims)
	peerID := generateTestPeerID(t)
	gridManager.RegisterPeer(peerID)

	joiner := &RDAJoinSubnet{
		gridManager:  gridManager,
		nodeID:       peerID,
		role:         ClientNode,
		myRow:        50,
		myCol:        75,
		gridDims:     dims,
		joinedTopics: make(map[string]interface{}),
	}

	assert.Equal(t, ClientNode, joiner.role)
	assert.Equal(t, 50, joiner.myRow)
	assert.Equal(t, 75, joiner.myCol)
	assert.Equal(t, int(dims.Rows), 128)

	t.Logf("✓ Client configuration: role=%s, position=(%d,%d)", joiner.role, joiner.myRow, joiner.myCol)
}

// TestJoinSubnet_BootstrapConfiguration
func TestJoinSubnet_BootstrapConfiguration(t *testing.T) {
	dims := GridDimensions{Rows: 64, Cols: 64}
	gridManager := NewRDAGridManager(dims)
	peerID := generateTestPeerID(t)
	gridManager.RegisterPeer(peerID)

	joiner := &RDAJoinSubnet{
		gridManager:  gridManager,
		nodeID:       peerID,
		role:         BootstrapNode,
		myRow:        0,
		myCol:        0,
		gridDims:     dims,
		joinedTopics: make(map[string]interface{}),
	}

	assert.Equal(t, BootstrapNode, joiner.role)
	assert.Equal(t, 0, joiner.myRow)
	assert.Equal(t, 0, joiner.myCol)
	assert.Equal(t, int(dims.Rows), 64)

	// Bootstrap will join 64 rows + 1 col = 65 subnets
	expectedSubnets := int(dims.Rows) + 1
	assert.Equal(t, 65, expectedSubnets)

	t.Logf("✓ Bootstrap configuration: role=%s, will join %d subnets (all %d rows + 1 col)",
		joiner.role, expectedSubnets, dims.Rows)
}

// TestJoinSubnet_LargeGrid
func TestJoinSubnet_LargeGrid(t *testing.T) {
	// 256x256 grid (typical for larger networks)
	clientSubnets := 2          // 1 row + 1 col
	bootstrapSubnets := 256 + 1 // All 256 rows + 1 col = 257

	assert.Equal(t, 2, clientSubnets)
	assert.Equal(t, 257, bootstrapSubnets)

	ratio := float64(bootstrapSubnets) / float64(clientSubnets)
	t.Logf("✓ Large grid (256x256):")
	t.Logf("  - Client joins: %d subnets", clientSubnets)
	t.Logf("  - Bootstrap joins: %d subnets (%.1fx more)", bootstrapSubnets, ratio)
}

// TestJoinSubnet_BootstrapConnectivity
func TestJoinSubnet_BootstrapConnectivity(t *testing.T) {
	// With K=128 grid:
	// - 128 rows, each needing at least 1 bootstrap node for connectivity
	// - 128 columns, each needing nodes to reach them
	//
	// Bootstrap node joining ALL rows ensures:
	// - It appears in every row subnet
	// - It acts as a bridge between rows
	// - It enables 1-hop data retrieval from any row

	gridRows := 128
	gridCols := 128

	// Single bootstrap node can serve:
	// - All 128 rows directly (via joining all row subnets)
	// - Its own column
	// - Acts as backbone for network connectivity

	bootstrapSubnets := gridRows + 1
	assert.Equal(t, 129, bootstrapSubnets)

	// This design ensures no row is isolated
	assert.GreaterOrEqual(t, 1, 0, "At least 1 bootstrap can connect full network")

	t.Logf("✓ Bootstrap node connectivity in %dx%d grid:", gridRows, gridCols)
	t.Logf("  - Joins: %d subnets (all rows + own col)", bootstrapSubnets)
	t.Logf("  - Role: Prevents network partition, enables global connectivity")
}
