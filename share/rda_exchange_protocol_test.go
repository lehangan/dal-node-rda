package share

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestRDAExchange_MessageStructures tests message format definitions
func TestRDAExchange_MessageStructures(t *testing.T) {
	t.Log("========== TEST: RDA Exchange Message Structures ==========")

	// Test RDASymbol structure
	symbol := RDASymbol{
		Handle:      "QmDataRoot123",
		ShareIndex:  42,
		Row:         0,
		Col:         42,
		ShareData:   []byte("sample share data"),
		ShareSize:   256,
		Timestamp:   time.Now().UnixNano() / 1e6,
		BlockHeight: 1000,
		NMTProof: NMTProofData{
			Nodes:       [][]byte{[]byte("node1"), []byte("node2")},
			LeafIndex:   42,
			LeafSize:    256,
			NamespaceID: "nmt-ns-1",
			RootHash:    []byte("root-hash-data"),
			Nonce:       1,
		},
	}

	// Serialize to JSON
	data, err := json.Marshal(symbol)
	assert.NoError(t, err)
	assert.NotEmpty(t, data)

	// Deserialize back
	var recovered RDASymbol
	err = json.Unmarshal(data, &recovered)
	assert.NoError(t, err)
	assert.Equal(t, symbol.Handle, recovered.Handle)
	assert.Equal(t, symbol.ShareIndex, recovered.ShareIndex)
	assert.Equal(t, symbol.Row, recovered.Row)
	assert.Equal(t, symbol.Col, recovered.Col)

	t.Log("✓ RDASymbol structure serializable")

	// Test StoreRequest
	storeReq := StoreRequest{
		RequestID: "store-001",
		Symbol:    symbol,
		Timestamp: time.Now().UnixNano() / 1e6,
		TTL:       3600,
		Priority:  1,
	}

	storeData, err := json.Marshal(storeReq)
	assert.NoError(t, err)
	t.Log("✓ StoreRequest structure serializable")

	// Test GetRequest
	getReq := GetRequest{
		RequestID:  "get-001",
		Handle:     "QmDataRoot123",
		ShareIndex: 42,
		Timestamp:  time.Now().UnixNano() / 1e6,
	}

	getData, err := json.Marshal(getReq)
	assert.NoError(t, err)
	t.Log("✓ GetRequest structure serializable")

	// Test SyncRequest
	syncReq := SyncRequest{
		RequestID: "sync-001",
		Symbols: []SymbolIdentifier{
			{Handle: "QmDataRoot123", ShareIndex: 1},
			{Handle: "QmDataRoot123", ShareIndex: 2},
			{Handle: "QmDataRoot123", ShareIndex: 3},
		},
		BatchSize: 10,
		Timestamp: time.Now().UnixNano() / 1e6,
	}

	syncData, err := json.Marshal(syncReq)
	assert.NoError(t, err)
	t.Log("✓ SyncRequest structure serializable")

	t.Logf("✓ Message sizes: StoreReq=%d, GetReq=%d, SyncReq=%d bytes", len(storeData), len(getData), len(syncData))
	t.Log("✓ Test PASSED: All message structures valid\n")
}

// TestRDAExchange_PredicateValidation tests Pred(h, i, x) validation
func TestRDAExchange_PredicateValidation(t *testing.T) {
	t.Log("========== TEST: Predicate Validation - Pred(h, i, x) ==========")

	checker := NewRDAPredicateChecker(256)

	// Create valid symbol
	validSymbol := &RDASymbol{
		Handle:     "QmDataRoot123",
		ShareIndex: 42,
		Row:        0,
		Col:        42,
		ShareData:  []byte("valid share data"),
		Timestamp:  time.Now().UnixNano() / 1e6,
		NMTProof: NMTProofData{
			Nodes:       [][]byte{[]byte("node1")},
			RootHash:    []byte("root"),
			NamespaceID: "ns-1",
		},
	}

	// Test 1: Valid symbol should pass
	result := checker.Pred("QmDataRoot123", 42, validSymbol)
	assert.True(t, result, "Valid symbol should pass Pred check")
	t.Log("✓ Valid symbol PASSED Pred(h, i, x)")

	// Test 2: Nil symbol should fail
	result = checker.Pred("QmDataRoot123", 42, nil)
	assert.False(t, result, "Nil symbol should fail Pred check")
	t.Log("✓ Nil symbol FAILED Pred (as expected)")

	// Test 3: Handle mismatch should fail
	wrongHandle := &RDASymbol{
		Handle:     "QmDataRootWrong",
		ShareIndex: 42,
		Row:        0,
		Col:        42,
		ShareData:  []byte("data"),
		NMTProof:   validSymbol.NMTProof,
	}
	result = checker.Pred("QmDataRoot123", 42, wrongHandle)
	assert.False(t, result, "Handle mismatch should fail")
	t.Log("✓ Handle mismatch FAILED Pred (as expected)")

	// Test 4: ShareIndex mismatch should fail
	wrongIndex := &RDASymbol{
		Handle:     "QmDataRoot123",
		ShareIndex: 99,
		Row:        0,
		Col:        99,
		ShareData:  []byte("data"),
		NMTProof:   validSymbol.NMTProof,
	}
	result = checker.Pred("QmDataRoot123", 42, wrongIndex)
	assert.False(t, result, "ShareIndex mismatch should fail")
	t.Log("✓ ShareIndex mismatch FAILED Pred (as expected)")

	// Test 5: Column mapping error should fail
	wrongCol := &RDASymbol{
		Handle:     "QmDataRoot123",
		ShareIndex: 42,
		Row:        0,
		Col:        50, // Should be 42
		ShareData:  []byte("data"),
		NMTProof:   validSymbol.NMTProof,
	}
	result = checker.Pred("QmDataRoot123", 42, wrongCol)
	assert.False(t, result, "Column mapping error should fail")
	t.Log("✓ Column mapping error FAILED Pred (as expected)")

	// Test 6: Empty share data should fail
	emptyData := &RDASymbol{
		Handle:     "QmDataRoot123",
		ShareIndex: 42,
		Row:        0,
		Col:        42,
		ShareData:  []byte{},
		NMTProof:   validSymbol.NMTProof,
	}
	result = checker.Pred("QmDataRoot123", 42, emptyData)
	assert.False(t, result, "Empty share data should fail")
	t.Log("✓ Empty data FAILED Pred (as expected)")

	t.Log("✓ Test PASSED: Predicate validation works correctly\n")
}

// TestRDAExchange_CellMapping tests Cell(i) function
func TestRDAExchange_CellMapping(t *testing.T) {
	t.Log("========== TEST: Cell(i) Index to Column Mapping ==========")

	gridSize := uint32(256)

	testCases := []struct {
		shareIndex  uint32
		expectedCol uint32
		description string
	}{
		{0, 0, "First share (row=0, col=0)"},
		{1, 1, "Second share (row=0, col=1)"},
		{255, 255, "Last share in first row (row=0, col=255)"},
		{256, 0, "First share in second row (row=1, col=0)"},
		{257, 1, "Second share in second row (row=1, col=1)"},
		{512, 0, "First share in third row (row=2, col=0)"},
		{42, 42, "Index 42 maps to col 42"},
		{65536, 0, "Large index: (row=256, col=0)"},
		{65600, 64, "Large index: (row=256, col=64)"},
	}

	for _, tc := range testCases {
		col := Cell(tc.shareIndex, gridSize)
		assert.Equal(t, tc.expectedCol, col, tc.description)
		t.Logf("✓ Cell(%d) = %d (%s)", tc.shareIndex, col, tc.description)
	}

	// Test Row function
	row := Row(256, gridSize)
	assert.Equal(t, uint32(1), row, "Row(256) should be 1")
	t.Log("✓ Row(256) = 1")

	// Test Index function (inverse mapping)
	index := IndexFromRowCol(0, 42, gridSize)
	assert.Equal(t, uint32(42), index, "Index(0, 42) should be 42")
	t.Log("✓ Index(0, 42) = 42")

	index = IndexFromRowCol(1, 0, gridSize)
	assert.Equal(t, uint32(256), index, "Index(1, 0) should be 256")
	t.Log("✓ Index(1, 0) = 256")

	// Test round-trip consistency
	for i := uint32(0); i < 65536; i += 1000 {
		row := Row(i, gridSize)
		col := Cell(i, gridSize)
		recoveredIndex := IndexFromRowCol(row, col, gridSize)
		assert.Equal(t, i, recoveredIndex, fmt.Sprintf("Round-trip failed for index %d", i))
	}
	t.Log("✓ Round-trip consistency verified")

	t.Log("✓ Test PASSED: Cell mapping works correctly\n")
}

// TestRDAExchange_StoreOperation tests STORE operation
func TestRDAExchange_StoreOperation(t *testing.T) {
	t.Log("========== TEST: STORE Operation ==========")

	// Create mock storage
	storage := NewMockRDAStorage()
	manager := NewRDAExchangeManager(256, storage)

	// Create store request
	storeReq := &StoreRequest{
		RequestID: "store-test-1",
		Symbol: RDASymbol{
			Handle:     "QmDataRoot001",
			ShareIndex: 42,
			Row:        0,
			Col:        42,
			ShareData:  []byte("test share data"),
			Timestamp:  time.Now().UnixNano() / 1e6,
			NMTProof: NMTProofData{
				Nodes:       [][]byte{[]byte("n1")},
				RootHash:    []byte("root"),
				NamespaceID: "ns-1",
			},
		},
		Timestamp: time.Now().UnixNano() / 1e6,
		TTL:       3600,
		Priority:  1,
	}

	// Handle store request
	resp := manager.HandleStoreRequest(storeReq)

	assert.True(t, resp.Success, "STORE should succeed")
	assert.NoError(t, manager.storageHandler.(*mockRDAStorage).LastError)
	assert.NotEmpty(t, resp.StorageID)

	t.Log("✓ STORE operation successful")
	t.Log("✓ Test PASSED: STORE works correctly\n")
}

// TestRDAExchange_GetOperation tests GET operation
func TestRDAExchange_GetOperation(t *testing.T) {
	t.Log("========== TEST: GET Operation ==========")

	storage := NewMockRDAStorage()
	manager := NewRDAExchangeManager(256, storage)

	// First, store a symbol
	symbol := &RDASymbol{
		Handle:     "QmDataRoot001",
		ShareIndex: 42,
		Row:        0,
		Col:        42,
		ShareData:  []byte("test share data"),
		Timestamp:  time.Now().UnixNano() / 1e6,
		NMTProof: NMTProofData{
			Nodes:       [][]byte{[]byte("n1")},
			RootHash:    []byte("root"),
			NamespaceID: "ns-1",
		},
	}

	err := storage.Store(symbol)
	assert.NoError(t, err)

	// Now GET it
	getReq := &GetRequest{
		RequestID:  "get-test-1",
		Handle:     "QmDataRoot001",
		ShareIndex: 42,
		Timestamp:  time.Now().UnixNano() / 1e6,
	}

	resp := manager.HandleGetRequest(getReq)

	assert.True(t, resp.Success, "GET should succeed")
	assert.NotNil(t, resp.Symbol)
	assert.Equal(t, "QmDataRoot001", resp.Symbol.Handle)
	assert.Equal(t, uint32(42), resp.Symbol.ShareIndex)
	assert.Equal(t, []byte("test share data"), resp.Symbol.ShareData)

	t.Log("✓ GET operation successful")
	t.Log("✓ Retrieved correct symbol")
	t.Log("✓ Test PASSED: GET works correctly\n")
}

// TestRDAExchange_SyncOperation tests SYNC operation
func TestRDAExchange_SyncOperation(t *testing.T) {
	t.Log("========== TEST: SYNC Operation ==========")

	storage := NewMockRDAStorage()
	manager := NewRDAExchangeManager(256, storage)

	// Store multiple symbols
	for i := uint32(0); i < 10; i++ {
		symbol := &RDASymbol{
			Handle:     "QmDataRoot001",
			ShareIndex: i,
			Row:        0,
			Col:        i,
			ShareData:  []byte(fmt.Sprintf("share data %d", i)),
			Timestamp:  time.Now().UnixNano() / 1e6,
			NMTProof: NMTProofData{
				Nodes:       [][]byte{[]byte("n1")},
				RootHash:    []byte("root"),
				NamespaceID: "ns-1",
			},
		}
		err := storage.Store(symbol)
		assert.NoError(t, err)
	}

	// Sync request for multiple symbols
	syncReq := &SyncRequest{
		RequestID: "sync-test-1",
		Symbols: []SymbolIdentifier{
			{Handle: "QmDataRoot001", ShareIndex: 0},
			{Handle: "QmDataRoot001", ShareIndex: 2},
			{Handle: "QmDataRoot001", ShareIndex: 5},
			{Handle: "QmDataRoot001", ShareIndex: 9},
		},
		BatchSize: 10,
		Timestamp: time.Now().UnixNano() / 1e6,
	}

	responses := manager.HandleSyncRequest(syncReq)

	assert.NotEmpty(t, responses, "Should have responses")
	assert.Greater(t, len(responses), 0, "Should have at least 1 response")

	for _, resp := range responses {
		assert.True(t, resp.Success)
		assert.NotNil(t, resp.Symbol)
	}

	t.Logf("✓ SYNC retrieved %d symbols", len(responses))
	t.Log("✓ Test PASSED: SYNC works correctly\n")
}

// TestRDAExchange_Statistics tests statistics tracking
func TestRDAExchange_Statistics(t *testing.T) {
	t.Log("========== TEST: Exchange Statistics ==========")

	storage := NewMockRDAStorage()
	manager := NewRDAExchangeManager(256, storage)

	// Execute operations
	for i := 0; i < 5; i++ {
		storeReq := &StoreRequest{
			RequestID: fmt.Sprintf("store-%d", i),
			Symbol: RDASymbol{
				Handle:     "QmDataRoot",
				ShareIndex: uint32(i),
				Row:        0,
				Col:        uint32(i),
				ShareData:  []byte("data"),
				NMTProof: NMTProofData{
					Nodes:       [][]byte{[]byte("n")},
					RootHash:    []byte("root"),
					NamespaceID: "ns",
				},
			},
		}
		manager.HandleStoreRequest(storeReq)
	}

	// Get stats
	stats := manager.GetStats()

	assert.Equal(t, int64(5), stats["total_queries"])
	assert.Equal(t, int64(5), stats["successful"])
	assert.Equal(t, int64(0), stats["failed"])
	assert.Equal(t, uint32(256), stats["grid_size"])

	t.Logf("✓ Stats: queries=%v, success=%v, grid=%v",
		stats["total_queries"],
		stats["successful"],
		stats["grid_size"],
	)

	t.Log("✓ Test PASSED: Statistics tracking works\n")
}

// TestRDAExchange_LargeGridScaling tests with large grid
func TestRDAExchange_LargeGridScaling(t *testing.T) {
	t.Log("========== TEST: Large Grid Scaling (256×256) ==========")

	checker := NewRDAPredicateChecker(256)

	// Create symbols for large grid
	totalSymbols := 256 * 256
	testIndices := []uint32{0, 1, 255, 256, 512, 65535}

	for _, idx := range testIndices {
		row := Row(idx, 256)
		col := Cell(idx, 256)

		symbol := &RDASymbol{
			Handle:     "QmDataRoot256x256",
			ShareIndex: idx,
			Row:        row,
			Col:        col,
			ShareData:  []byte("scaled data"),
			NMTProof: NMTProofData{
				Nodes:       [][]byte{[]byte("n")},
				RootHash:    []byte("root"),
				NamespaceID: "ns",
			},
		}

		result := checker.Pred("QmDataRoot256x256", idx, symbol)
		assert.True(t, result, fmt.Sprintf("Pred should pass for index %d", idx))
	}

	t.Logf("✓ Tested %d index mappings in 256×256 grid", len(testIndices))
	t.Logf("✓ Total possible symbols: %d (256 rows × 256 cols)", totalSymbols)
	t.Log("✓ Test PASSED: Large grid scaling works\n")
}

// ============================================================================
// Mock Storage for Testing
// ============================================================================

type mockRDAStorage struct {
	symbols   map[string]*RDASymbol
	LastError error
}

func NewMockRDAStorage() *mockRDAStorage {
	return &mockRDAStorage{
		symbols: make(map[string]*RDASymbol),
	}
}

func (m *mockRDAStorage) Store(symbol *RDASymbol) error {
	key := fmt.Sprintf("%s_%d", symbol.Handle, symbol.ShareIndex)
	m.symbols[key] = symbol
	return nil
}

func (m *mockRDAStorage) Get(handle string, shareIndex uint32) (*RDASymbol, error) {
	key := fmt.Sprintf("%s_%d", handle, shareIndex)
	if sym, ok := m.symbols[key]; ok {
		return sym, nil
	}
	return nil, fmt.Errorf("symbol not found")
}

func (m *mockRDAStorage) GetBatch(symbols []SymbolIdentifier) ([]*RDASymbol, error) {
	result := make([]*RDASymbol, 0)
	for _, id := range symbols {
		key := fmt.Sprintf("%s_%d", id.Handle, id.ShareIndex)
		if sym, ok := m.symbols[key]; ok {
			result = append(result, sym)
		}
	}
	return result, nil
}
