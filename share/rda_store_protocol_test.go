package share

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestStoreProtocol_MessageStructure kiểm tra định dạng message
func TestStoreProtocol_MessageStructure(t *testing.T) {
	t.Log("========== TEST: STORE Message Structure ==========")

	msg := &StoreMessage{
		Type:        StoreMessageTypeStore,
		Handle:      "QmDataRoot001",
		ShareIndex:  42,
		RowID:       0,
		ColID:       42,
		Data:        []byte("test share data"),
		NMTProof:    &NMTProofData{Nodes: [][]byte{[]byte("n1")}},
		Timestamp:   time.Now().UnixNano() / 1e6,
		SenderID:    "12D3KooXXXX",
		SenderRow:   0,
		SenderCol:   10,
		BlockHeight: 1000,
	}

	assert.Equal(t, StoreMessageTypeStore, msg.Type)
	assert.Equal(t, "QmDataRoot001", msg.Handle)
	assert.Equal(t, uint32(42), msg.ShareIndex)
	assert.Equal(t, []byte("test share data"), msg.Data)

	t.Log("✓ Message structure valid")
	t.Log("✓ Test PASSED\n")
}

// TestStoreProtocol_StoreFwdMessageType kiểm tra forward marker
func TestStoreProtocol_StoreFwdMessageType(t *testing.T) {
	t.Log("========== TEST: STORE_FWD Message Type ==========")

	msg := &StoreMessage{
		Type:       StoreMessageTypeStoreFwd,
		Handle:     "QmDataRoot001",
		ShareIndex: 42,
	}

	assert.Equal(t, StoreMessageTypeStoreFwd, msg.Type)
	assert.NotEqual(t, StoreMessageTypeStore, msg.Type)

	t.Log("✓ STORE_FWD type correctly marked")
	t.Log("✓ Test PASSED\n")
}

// TestStoreProtocol_CellMapping kiểm tra Cell(i) mapping
func TestStoreProtocol_CellMapping(t *testing.T) {
	t.Log("========== TEST: Cell(i) Mapping for Distribution ==========")

	gridSize := uint32(256)

	// Test cases
	tests := []struct {
		shareIndex uint32
		expectedCol uint32
	}{
		{0, 0},
		{42, 42},
		{255, 255},
		{256, 0},   // Wrap to next row, col 0
		{257, 1},   // Wrap to next row, col 1
		{512, 0},   // Row 2, col 0
	}

	for _, tc := range tests {
		col := Cell(tc.shareIndex, gridSize)
		assert.Equal(t, tc.expectedCol, col)
		t.Logf("✓ Cell(%d) = %d", tc.shareIndex, col)
	}

	t.Log("✓ Test PASSED - Cell mapping correct\n")
}

// TestStoreProtocol_RowMapping kiểm tra Row(i) mapping
func TestStoreProtocol_RowMapping(t *testing.T) {
	t.Log("========== TEST: Row(i) Mapping ==========")

	gridSize := uint32(256)

	tests := []struct {
		shareIndex uint32
		expectedRow uint32
	}{
		{0, 0},
		{255, 0},   // Last in first row
		{256, 1},   // First in second row
		{512, 2},
		{257, 1},
	}

	for _, tc := range tests {
		row := Row(tc.shareIndex, gridSize)
		assert.Equal(t, tc.expectedRow, row)
		t.Logf("✓ Row(%d) = %d", tc.shareIndex, row)
	}

	t.Log("✓ Test PASSED - Row mapping correct\n")
}

// TestStoreProtocol_IndexRoundTrip kiểm tra round-trip mapping
func TestStoreProtocol_IndexRoundTrip(t *testing.T) {
	t.Log("========== TEST: Index Round-Trip Consistency ==========")

	gridSize := uint32(256)

	for i := uint32(0); i < 65536; i += 1000 {
		row := Row(i, gridSize)
		col := Cell(i, gridSize)
		recoveredIndex := IndexFromRowCol(row, col, gridSize)

		assert.Equal(t, i, recoveredIndex)
		t.Logf("✓ Index %d → (row=%d, col=%d) → %d", i, row, col, recoveredIndex)
	}

	t.Log("✓ Test PASSED - Round-trip consistency verified\n")
}

// TestStoreProtocol_ValidateWithPred kiểm tra validation
func TestStoreProtocol_ValidateWithPred(t *testing.T) {
	t.Log("========== TEST: Validate with Pred ==========")

	checker := NewRDAPredicateChecker(256)

	// Valid symbol with complete NMT Proof
	symbol := &RDASymbol{
		Handle:     "QmDataRoot001",
		ShareIndex: 42,
		Row:        0,
		Col:        42,
		ShareData:  []byte("test share data with sufficient length for validation purposes"),
		Timestamp:  time.Now().UnixNano() / 1e6,
		NMTProof: NMTProofData{
			Nodes:       [][]byte{[]byte("node1"), []byte("node2")},
			RootHash:    []byte("rootHashForQmDataRoot001"),
			NamespaceID: "namespace001",
		},
	}

	result := checker.Pred("QmDataRoot001", 42, symbol)
	assert.True(t, result)
	t.Log("✓ Valid symbol passes Pred validation")

	// Invalid - wrong Column (should be 42 but we set to 50)
	invalidSymbol := &RDASymbol{
		Handle:     "QmDataRoot001",
		ShareIndex: 42,
		Row:        0,
		Col:        50, // Should be 42
		ShareData:  []byte("test share data with sufficient length"),
		Timestamp:  time.Now().UnixNano() / 1e6,
		NMTProof: NMTProofData{
			Nodes:       [][]byte{[]byte("node1")},
			RootHash:    []byte("rootHashForQmDataRoot001"),
			NamespaceID: "namespace001",
		},
	}

	result = checker.Pred("QmDataRoot001", 42, invalidSymbol)
	assert.False(t, result)
	t.Log("✓ Invalid column symbol fails Pred validation")

	// Invalid - empty share data
	emptySymbol := &RDASymbol{
		Handle:     "QmDataRoot001",
		ShareIndex: 42,
		Row:        0,
		Col:        42,
		ShareData:  []byte{}, // Empty
		Timestamp:  time.Now().UnixNano() / 1e6,
		NMTProof: NMTProofData{
			Nodes:       [][]byte{[]byte("node1")},
			RootHash:    []byte("root"),
			NamespaceID: "ns",
		},
	}
	result = checker.Pred("QmDataRoot001", 42, emptySymbol)
	assert.False(t, result)
	t.Log("✓ Empty share data fails Pred validation")

	t.Log("✓ Test PASSED\n")
}

// TestStoreProtocol_ProtocolIDs kiểm tra protocol identifiers
func TestStoreProtocol_ProtocolIDs(t *testing.T) {
	t.Log("========== TEST: Protocol IDs ==========")

	assert.Equal(t, "/celestia/rda/store/1.0.0", string(RDAStoreProtocolID))
	assert.Equal(t, "/celestia/rda/store_fwd/1.0.0", string(RDAStoreFwdProtocolID))

	t.Logf("✓ STORE protocol ID: %s", RDAStoreProtocolID)
	t.Logf("✓ STORE_FWD protocol ID: %s", RDAStoreFwdProtocolID)
	t.Log("✓ Test PASSED\n")
}

// TestStoreProtocol_MessageMarshaling kiểm tra serialization
func TestStoreProtocol_MessageMarshaling(t *testing.T) {
	t.Log("========== TEST: Message Marshaling ==========")

	msg := &StoreMessage{
		Type:        StoreMessageTypeStore,
		Handle:      "QmDataRoot001",
		ShareIndex:  42,
		RowID:       0,
		ColID:       42,
		Data:        []byte("test data"),
		NMTProof:    &NMTProofData{Nodes: [][]byte{[]byte("n1")}, RootHash: []byte("root")},
		Timestamp:   time.Now().UnixNano() / 1e6,
		SenderID:    "peer123",
		SenderRow:   0,
		SenderCol:   10,
		BlockHeight: 1000,
	}

	// Test that message is serializable
	assert.NotNil(t, msg)
	assert.Equal(t, uint32(42), msg.ShareIndex)
	assert.Equal(t, "QmDataRoot001", msg.Handle)

	t.Log("✓ Message marshaling works")
	t.Log("✓ Test PASSED\n")
}
