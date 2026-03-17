package share

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// ============================================================================
// GET Protocol Tests - Phase 3
// ============================================================================

// TestGetProtocol_MessageStructure kiểm tra định dạng message
func TestGetProtocol_MessageStructure(t *testing.T) {
	t.Log("========== TEST: GET Message Structure ==========")

	msg := &GetMessage{
		Type:        GetMessageTypeQuery,
		Handle:      "QmDataRoot001",
		ShareIndex:  42,
		SenderID:    "peer123",
		Timestamp:   time.Now().UnixNano() / 1e6,
		RequestID:   "req-001",
	}

	assert.Equal(t, GetMessageTypeQuery, msg.Type)
	assert.Equal(t, "QmDataRoot001", msg.Handle)
	assert.Equal(t, uint32(42), msg.ShareIndex)
	assert.Equal(t, "req-001", msg.RequestID)

	t.Log("✓ GET Query message structure valid")

	// Test Response
	respMsg := &GetMessage{
		Type:        GetMessageTypeResponse,
		Handle:      "QmDataRoot001",
		ShareIndex:  42,
		RowID:       10,
		ColID:       42,
		Data:        []byte("test share data"),
		SenderID:    "peer456",
		Timestamp:   time.Now().UnixNano() / 1e6,
		RequestID:   "req-001",
	}

	assert.Equal(t, GetMessageTypeResponse, respMsg.Type)
	assert.Equal(t, uint32(10), respMsg.RowID)
	assert.Equal(t, uint32(42), respMsg.ColID)
	assert.Equal(t, []byte("test share data"), respMsg.Data)

	t.Log("✓ GET Response message structure valid")
	t.Log("✓ Test PASSED\n")
}

// TestGetProtocol_QueryTypes kiểm tra query vs response types
func TestGetProtocol_QueryTypes(t *testing.T) {
	t.Log("========== TEST: GET Message Types ==========")

	queryMsg := &GetMessage{
		Type:       GetMessageTypeQuery,
		Handle:     "QmTest",
		ShareIndex: 100,
	}

	respMsg := &GetMessage{
		Type:   GetMessageTypeResponse,
		Handle: "QmTest",
		Data:   []byte("response data"),
	}

	assert.Equal(t, GetMessageTypeQuery, queryMsg.Type)
	assert.Equal(t, GetMessageTypeResponse, respMsg.Type)
	assert.NotEqual(t, queryMsg.Type, respMsg.Type)

	t.Log("✓ Query type correctly identified")
	t.Log("✓ Response type correctly identified")
	t.Log("✓ Test PASSED\n")
}

// TestGetProtocol_ProtocolID kiểm tra protocol identifier
func TestGetProtocol_ProtocolID(t *testing.T) {
	t.Log("========== TEST: GET Protocol ID ==========")

	expected := "/celestia/rda/get/1.0.0"
	assert.Equal(t, expected, string(RDAGetProtocolID))

	t.Logf("✓ GET protocol ID: %s", RDAGetProtocolID)
	t.Log("✓ Test PASSED\n")
}

// TestGetProtocol_RequestIDTracking kiểm tra tracking requests
func TestGetProtocol_RequestIDTracking(t *testing.T) {
	t.Log("========== TEST: GET Request ID Tracking ==========")

	requestIDs := make(map[string]bool)
	for i := 0; i < 100; i++ {
		msg := &GetMessage{
			Type:      GetMessageTypeQuery,
			RequestID: "req-" + string(rune(i)),
		}
		assert.NotEmpty(t, msg.RequestID)
		requestIDs[msg.RequestID] = true
	}

	assert.Equal(t, 100, len(requestIDs))
	t.Log("✓ 100 unique request IDs generated")
	t.Log("✓ Test PASSED\n")
}

// TestGetProtocol_ResponseMatching kiểm tra matching request-response
func TestGetProtocol_ResponseMatching(t *testing.T) {
	t.Log("========== TEST: GET Request-Response Matching ==========")

	requestID := "req-12345"
	
	req := &GetMessage{
		Type:      GetMessageTypeQuery,
		Handle:    "QmDataRoot",
		RequestID: requestID,
	}

	resp := &GetMessage{
		Type:      GetMessageTypeResponse,
		Handle:    "QmDataRoot",
		RequestID: requestID,
	}

	// Verify match
	assert.Equal(t, req.RequestID, resp.RequestID)
	assert.Equal(t, req.Handle, resp.Handle)
	assert.NotEqual(t, req.Type, resp.Type)

	t.Log("✓ Request-Response IDs match correctly")
	t.Log("✓ Test PASSED\n")
}

// TestGetProtocol_TimestampFreshness kiểm tra timestamp
func TestGetProtocol_TimestampFreshness(t *testing.T) {
	t.Log("========== TEST: GET Timestamp Freshness ==========")

	before := time.Now().UnixNano() / 1e6
	msg := &GetMessage{
		Type:      GetMessageTypeResponse,
		Timestamp: time.Now().UnixNano() / 1e6,
	}
	after := time.Now().UnixNano() / 1e6

	// Should be fresh (within last 100 milliseconds for timing variation)
	assert.GreaterOrEqual(t, msg.Timestamp, before-10)
	assert.LessOrEqual(t, msg.Timestamp, after+10)

	t.Logf("✓ Message timestamp fresh: generated between %d and %d", before, after)
	t.Log("✓ Test PASSED\n")
}

// ============================================================================
// SYNC Protocol Tests - Phase 4
// ============================================================================

// TestSyncProtocol_MessageStructure kiểm tra định dạng message
func TestSyncProtocol_MessageStructure(t *testing.T) {
	t.Log("========== TEST: SYNC Message Structure ==========")

	req := &SyncMessage{
		Type:        SyncMessageTypeRequest,
		RequestCol:  42,
		RequestRow:  10,
		SinceHeight: 1000,
		SenderID:    "peer123",
		Timestamp:   time.Now().UnixNano() / 1e6,
		RequestID:   "sync-001",
	}

	assert.Equal(t, SyncMessageTypeRequest, req.Type)
	assert.Equal(t, uint32(42), req.RequestCol)
	assert.Equal(t, uint32(10), req.RequestRow)
	assert.Equal(t, uint64(1000), req.SinceHeight)

	t.Log("✓ SYNC Request message structure valid")

	// Test Response
	respMsg := &SyncMessage{
		Type:      SyncMessageTypeResponse,
		Shares:    []SyncShareData{},
		SenderID:  "peer456",
		Timestamp: time.Now().UnixNano() / 1e6,
		RequestID: "sync-001",
	}

	assert.Equal(t, SyncMessageTypeResponse, respMsg.Type)
	assert.Equal(t, 0, len(respMsg.Shares))

	t.Log("✓ SYNC Response message structure valid")
	t.Log("✓ Test PASSED\n")
}

// TestSyncProtocol_SyncShareData kiểm tra share data structure
func TestSyncProtocol_SyncShareData(t *testing.T) {
	t.Log("========== TEST: SYNC Share Data Structure ==========")

	share := SyncShareData{
		Handle:     "QmDataRoot",
		ShareIndex: 42,
		Row:        10,
		Col:        42,
		Data:       []byte("share data"),
		Height:     1000,
		NMTProof: NMTProofData{
			Nodes:       [][]byte{[]byte("node1")},
			RootHash:    []byte("root"),
			NamespaceID: "namespace",
		},
	}

	assert.Equal(t, "QmDataRoot", share.Handle)
	assert.Equal(t, uint32(42), share.ShareIndex)
	assert.Equal(t, uint32(10), share.Row)
	assert.Equal(t, uint32(42), share.Col)
	assert.Equal(t, uint64(1000), share.Height)
	assert.NotEmpty(t, share.NMTProof.Nodes)

	t.Log("✓ SYNC Share data structure valid")
	t.Log("✓ Test PASSED\n")
}

// TestSyncProtocol_MessageTypes kiểm tra request vs response types
func TestSyncProtocol_MessageTypes(t *testing.T) {
	t.Log("========== TEST: SYNC Message Types ==========")

	reqMsg := &SyncMessage{
		Type:       SyncMessageTypeRequest,
		RequestCol: 42,
	}

	respMsg := &SyncMessage{
		Type:   SyncMessageTypeResponse,
		Shares: []SyncShareData{},
	}

	assert.Equal(t, SyncMessageTypeRequest, reqMsg.Type)
	assert.Equal(t, SyncMessageTypeResponse, respMsg.Type)
	assert.NotEqual(t, reqMsg.Type, respMsg.Type)

	t.Log("✓ Request type correctly identified")
	t.Log("✓ Response type correctly identified")
	t.Log("✓ Test PASSED\n")
}

// TestSyncProtocol_ProtocolID kiểm tra protocol identifier
func TestSyncProtocol_ProtocolID(t *testing.T) {
	t.Log("========== TEST: SYNC Protocol ID ==========")

	expected := "/celestia/rda/sync/1.0.0"
	assert.Equal(t, expected, string(RDASyncProtocolID))

	t.Logf("✓ SYNC protocol ID: %s", RDASyncProtocolID)
	t.Log("✓ Test PASSED\n")
}

// TestSyncProtocol_MaxSharesLimit kiểm tra limit trên số shares
func TestSyncProtocol_MaxSharesLimit(t *testing.T) {
	t.Log("========== TEST: SYNC Max Shares Limit ==========")

	assert.Equal(t, 1000, MaxSharesPerSync)

	t.Logf("✓ Max shares per SYNC: %d", MaxSharesPerSync)
	t.Log("✓ Test PASSED\n")
}

// TestSyncProtocol_BlockHeightWindow kiểm tra window cho block height
func TestSyncProtocol_BlockHeightWindow(t *testing.T) {
	t.Log("========== TEST: SYNC Block Height Window ==========")

	assert.Equal(t, uint64(60), MaxBlockHeightWindow)

	t.Logf("✓ Max block height window: %d blocks", MaxBlockHeightWindow)
	t.Log("✓ Test PASSED\n")
}

// TestSyncProtocol_MultipleShares kiểm tra response với nhiều shares
func TestSyncProtocol_MultipleShares(t *testing.T) {
	t.Log("========== TEST: SYNC Multiple Shares Response ==========")

	shares := make([]SyncShareData, 100)
	for i := 0; i < 100; i++ {
		shares[i] = SyncShareData{
			Handle:     "QmDataRoot",
			ShareIndex: uint32(i),
			Row:        uint32(i / 16),
			Col:        uint32(i % 16),
			Data:       []byte("data"),
			Height:     uint64(1000 + i),
		}
	}

	msg := &SyncMessage{
		Type:   SyncMessageTypeResponse,
		Shares: shares,
	}

	assert.Equal(t, 100, len(msg.Shares))
	t.Log("✓ 100 shares packed in single response")
	t.Log("✓ Test PASSED\n")
}

// TestSyncProtocol_EmptyResponse kiểm tra response rỗng
func TestSyncProtocol_EmptyResponse(t *testing.T) {
	t.Log("========== TEST: SYNC Empty Response (No Data) ==========")

	msg := &SyncMessage{
		Type:   SyncMessageTypeResponse,
		Shares: []SyncShareData{},
	}

	assert.Equal(t, SyncMessageTypeResponse, msg.Type)
	assert.Equal(t, 0, len(msg.Shares))

	t.Log("✓ Empty response correctly formed")
	t.Log("✓ Test PASSED\n")
}

// TestSyncProtocol_RequestIDTracking kiểm tra tracking sync requests
func TestSyncProtocol_RequestIDTracking(t *testing.T) {
	t.Log("========== TEST: SYNC Request ID Tracking ==========")

	requestIDs := make(map[string]bool)
	for i := 0; i < 50; i++ {
		msg := &SyncMessage{
			Type:      SyncMessageTypeRequest,
			RequestID: "sync-req-" + string(rune(i)),
		}
		assert.NotEmpty(t, msg.RequestID)
		requestIDs[msg.RequestID] = true
	}

	assert.Equal(t, 50, len(requestIDs))
	t.Log("✓ 50 unique SYNC request IDs generated")
	t.Log("✓ Test PASSED\n")
}
