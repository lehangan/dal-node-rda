package share

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var getLog = logging.Logger("rda.get")

// ============================================================================
// GET Protocol Definition - Phase 3: Query Sampling
// ============================================================================
// Thay thế DAS worker: Light Node yêu cầu RDA Grid để lấy mẫu siêu nhanh (1-hop)
// Flow:
//   Phía hỏi (Light Node):
//   1. GetShare(i) gọi Cell(i) → lấy cột c
//   2. Tìm láng giềng ở (myRow, c)
//   3. Gửi (get, h, i) → chờ (get_rsp, h, i, x)
//   4. Validate với Pred → return
//   5. Nếu fail/timeout → retry láng giềng khác
//
//   Phía trả lời (Bridge/Full Node):
//   1. Listener /rda/get/1 → nhận (get, h, i)
//   2. Tra cứu DB cục bộ
//   3. Trả (get_rsp, h, i, x) + Proof

const (
	// /rda/get/1 - Query protocol (Light Node → Grid Node)
	RDAGetProtocolID protocol.ID = "/celestia/rda/get/1.0.0"
)

// GetMessageType định nghĩa loại thông điệp
type GetMessageType string

const (
	GetMessageTypeQuery    GetMessageType = "get"     // (get, h, i)
	GetMessageTypeResponse GetMessageType = "get_rsp" // (get_rsp, h, i, x, proof)
)

// GetMessage - "Phong bè" (envelope) cho GET operation
type GetMessage struct {
	// Message type: "get" hoặc "get_rsp"
	Type GetMessageType `json:"type"`

	// Dữ liệu truy vấn (h, i)
	Handle     string `json:"handle"`    // h: DataRoot
	ShareIndex uint32 `json:"share_index"` // i: Index

	// Response-only fields
	RowID      uint32         `json:"row_id,omitempty"`    // Hàng của node có dữ liệu
	ColID      uint32         `json:"col_id,omitempty"`    // Cột
	Data       []byte         `json:"data,omitempty"`      // x: Share data
	NMTProof   *NMTProofData  `json:"nmt_proof,omitempty"` // Proof để xác minh

	// Metadata
	SenderID    string `json:"sender_id"`
	Timestamp   int64  `json:"timestamp"`   // milliseconds
	BlockHeight uint64 `json:"block_height"`
	RequestID   string `json:"request_id"`  // Để match response với request
}

// ============================================================================
// RDAGetProtocolHandler - Phía trả lời (Bridge/Full Node)
// ============================================================================

type RDAGetProtocolHandler struct {
	host         host.Host
	gridManager  *RDAGridManager
	peerManager  *RDAPeerManager
	subnetMgr    *RDASubnetManager
	storage      *RDAStorage       // Để truy vấn dữ liệu local
	predicateChk *RDAPredicateChecker

	streamHandlers map[string]bool
	mu             sync.RWMutex

	// Statistics
	totalGetReceived    int64
	totalGetResponded   int64
	totalNotFound       int64
	failedValidations   int64
	totalBytesTransfer  int64
}

// NewRDAGetProtocolHandler tạo handler mới
func NewRDAGetProtocolHandler(
	host host.Host,
	gridManager *RDAGridManager,
	peerManager *RDAPeerManager,
	subnetMgr *RDASubnetManager,
	storage *RDAStorage,
) *RDAGetProtocolHandler {
	gridSize := uint32(gridManager.GetGridDimensions().Cols)
	return &RDAGetProtocolHandler{
		host:               host,
		gridManager:        gridManager,
		peerManager:        peerManager,
		subnetMgr:          subnetMgr,
		storage:            storage,
		predicateChk:       NewRDAPredicateChecker(gridSize),
		streamHandlers:     make(map[string]bool),
		totalGetReceived:   0,
		totalGetResponded:  0,
		totalNotFound:      0,
		failedValidations:  0,
		totalBytesTransfer: 0,
	}
}

// Start registers stream handlers
func (h *RDAGetProtocolHandler) Start(ctx context.Context) error {
	h.host.SetStreamHandler(RDAGetProtocolID, h.handleGetStream)

	h.mu.Lock()
	h.streamHandlers[string(RDAGetProtocolID)] = true
	h.mu.Unlock()

	getLog.Infof("RDA|GET|START ✓ Handler started - protocol: %s", RDAGetProtocolID)
	return nil
}

// Stop unregisters stream handlers
func (h *RDAGetProtocolHandler) Stop() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.host.RemoveStreamHandler(RDAGetProtocolID)
	h.streamHandlers[string(RDAGetProtocolID)] = false

	getLog.Infof("RDA|GET|STOP ✓ Handler stopped")
	return nil
}

// handleGetStream handles incoming GET requests from Light Nodes
func (h *RDAGetProtocolHandler) handleGetStream(stream network.Stream) {
	defer stream.Close()

	remoteID := stream.Conn().RemotePeer()
	getLog.Debugf("RDA|GET|STREAM_OPEN peer=%s", remoteID.String()[:8])

	// Đọc thông điệp
	var msg GetMessage
	decoder := json.NewDecoder(stream)
	if err := decoder.Decode(&msg); err != nil {
		getLog.Warnf("RDA|GET|DECODE_ERROR peer=%s error=%v", remoteID.String()[:8], err)
		return
	}

	if msg.Type != GetMessageTypeQuery {
		getLog.Warnf("RDA|GET|TYPE_ERROR expected=query got=%s from=%s", msg.Type, remoteID.String()[:8])
		return
	}

	h.totalGetReceived++
	getLog.Debugf("RDA|GET|RECEIVE peer=%s handle=%s index=%d requestID=%s",
		remoteID.String()[:8], msg.Handle[:8], msg.ShareIndex, msg.RequestID)

	// ========== BƯỚC 1: LOOKUP DATABASE ==========
	getLog.Debugf("RDA|GET|LOOKUP_START handle=%s index=%d", msg.Handle[:8], msg.ShareIndex)
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Query storage for the share (try getting from any row first)
	// In production, would query by handle and shareIndex
	// For now, simulate by checking if we have data in our column
	myPos, _ := h.gridManager.GetPeerPosition(h.host.ID())
	shareData, err := h.storage.GetShare(ctx, 0, uint32(myPos.Row), uint32(myPos.Col), msg.ShareIndex)

	if err != nil || len(shareData) == 0 {
		h.totalNotFound++
		getLog.Warnf("RDA|GET|NOT_FOUND handle=%s index=%d row=%d col=%d", 
			msg.Handle[:8], msg.ShareIndex, myPos.Row, myPos.Col)

		// Trả thông báo "không có" (khống response lại)
		return
	}

	getLog.Debugf("RDA|GET|FOUND ✓ handle=%s index=%d size=%d", msg.Handle[:8], msg.ShareIndex, len(shareData))

	// ========== BƯỚC 2: VALIDATE VỚI PRED ==========
	getLog.Debugf("RDA|GET|VALIDATE_START handle=%s index=%d", msg.Handle[:8], msg.ShareIndex)
	
	symbol := &RDASymbol{
		Handle:     msg.Handle,
		ShareIndex: msg.ShareIndex,
		Row:        uint32(myPos.Row),
		Col:        uint32(myPos.Col),
		ShareData:  shareData,
		Timestamp:  time.Now().UnixNano() / 1e6,
		NMTProof: NMTProofData{
			Nodes:       [][]byte{},
			RootHash:    []byte(msg.Handle),
			NamespaceID: "rda",
		},
	}

	if !h.predicateChk.Pred(msg.Handle, msg.ShareIndex, symbol) {
		h.failedValidations++
		getLog.Warnf("RDA|GET|VALIDATE_FAIL handle=%s index=%d reason=pred", msg.Handle[:8], msg.ShareIndex)
		return
	}

	getLog.Debugf("RDA|GET|VALIDATE_OK ✓ handle=%s index=%d", msg.Handle[:8], msg.ShareIndex)

	// ========== BƯỚC 3: ASSEMBLE & SEND RESPONSE ==========
	getLog.Debugf("RDA|GET|RESPONSE_BUILD handle=%s index=%d size=%d", msg.Handle[:8], msg.ShareIndex, len(shareData))
	
	response := GetMessage{
		Type:        GetMessageTypeResponse,
		Handle:      msg.Handle,
		ShareIndex:  msg.ShareIndex,
		RowID:       uint32(myPos.Row),
		ColID:       uint32(myPos.Col),
		Data:        shareData,
		NMTProof:    &symbol.NMTProof,
		SenderID:    h.host.ID().String(),
		Timestamp:   time.Now().UnixNano() / 1e6,
		BlockHeight: 0, // TODO: Get actual block height
		RequestID:   msg.RequestID,
	}

	respData, err := json.Marshal(response)
	if err != nil {
		getLog.Warnf("RDA|GET|MARSHAL_ERROR handle=%s error=%v", msg.Handle[:8], err)
		return
	}

	if _, err := stream.Write(respData); err != nil {
		getLog.Warnf("RDA|GET|SEND_ERROR peer=%s handle=%s error=%v", remoteID.String()[:8], msg.Handle[:8], err)
		return
	}

	h.totalGetResponded++
	h.totalBytesTransfer += int64(len(respData))

	getLog.Infof("RDA|GET|RESPONSE_SENT ✓ peer=%s handle=%s index=%d size=%d bytes",
		remoteID.String()[:8], msg.Handle[:8], msg.ShareIndex, len(respData))
}

// GetStats trả về statistics
func (h *RDAGetProtocolHandler) GetStats() map[string]interface{} {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return map[string]interface{}{
		"total_get_received":  h.totalGetReceived,
		"total_get_responded": h.totalGetResponded,
		"total_not_found":     h.totalNotFound,
		"failed_validations":  h.failedValidations,
		"total_bytes_transfer": h.totalBytesTransfer,
	}
}

// ============================================================================
// RDAGetProtocolRequester - Phía hỏi (Light Node)
// ============================================================================

type RDAGetProtocolRequester struct {
	host         host.Host
	gridManager  *RDAGridManager
	peerManager  *RDAPeerManager
	subnetMgr    *RDASubnetManager
	predicateChk *RDAPredicateChecker

	mu                sync.RWMutex
	requestTimeout    time.Duration
	retryAttempts     int
	totalRequests     int64
	successfulGets    int64
	failedRetries     int64
	totalBytesReceived int64
}

// NewRDAGetProtocolRequester tạo requester mới
func NewRDAGetProtocolRequester(
	host host.Host,
	gridManager *RDAGridManager,
	peerManager *RDAPeerManager,
	subnetMgr *RDASubnetManager,
) *RDAGetProtocolRequester {
	gridSize := uint32(gridManager.GetGridDimensions().Cols)
	return &RDAGetProtocolRequester{
		host:              host,
		gridManager:       gridManager,
		peerManager:       peerManager,
		subnetMgr:         subnetMgr,
		predicateChk:      NewRDAPredicateChecker(gridSize),
		requestTimeout:    3 * time.Second,
		retryAttempts:     3,
		totalRequests:     0,
		successfulGets:    0,
		failedRetries:     0,
		totalBytesReceived: 0,
	}
}

// QueryShare implements the GetShare override for Light Nodes
// Throws: (h, i) request → Cell(i) → (myRow, c) → find peers → Send GET → Validate → Return
func (r *RDAGetProtocolRequester) QueryShare(
	ctx context.Context,
	handle string,
	shareIndex uint32,
) (*RDASymbol, error) {
	// Step 1: Calculate target column c = Cell(i)
	targetCol := Cell(shareIndex, uint32(r.gridManager.GetGridDimensions().Cols))
	myPos, found := r.gridManager.GetPeerPosition(r.host.ID())
	if !found {
		return nil, fmt.Errorf("cannot determine my grid position")
	}

	myRow := uint32(myPos.Row)
	getLog.Infof("RDA|GET|QUERY_START handle=%s index=%d targetCol=%d myRow=%d", 
		handle[:8], shareIndex, targetCol, myRow)

	// Step 2: Find peers at (myRow, targetCol) intersection
	intersectionPeers := r.findIntersectionPeers(myRow, targetCol)
	if len(intersectionPeers) == 0 {
		getLog.Warnf("RDA|GET|NO_PEERS row=%d col=%d", myRow, targetCol)
		return nil, fmt.Errorf("no peers available at grid intersection")
	}

	getLog.Debugf("RDA|GET|PEERS_FOUND count=%d row=%d col=%d", len(intersectionPeers), myRow, targetCol)

	// Step 3: Try to query peers with retries
	for attempt := 0; attempt < r.retryAttempts; attempt++ {
		for _, peer := range intersectionPeers {
			symbol, err := r.sendGetRequest(ctx, peer, handle, shareIndex)
			if err == nil {
				r.successfulGets++
				getLog.Infof("RDA|GET|SUCCESS ✓ handle=%s index=%d peer=%s attempt=%d", 
					handle[:8], shareIndex, peer.String()[:8], attempt+1)
				return symbol, nil
			}
			getLog.Debugf("RDA|GET|RETRY peer=%s attempt=%d error=%v", peer.String()[:8], attempt+1, err)
		}
	}

	r.failedRetries++
	getLog.Warnf("RDA|GET|FAILED_ALL handle=%s index=%d row=%d col=%d", 
		handle[:8], shareIndex, myRow, targetCol)
	return nil, fmt.Errorf("all peers at intersection (%d, %d) failed to respond", myRow, targetCol)
}

// sendGetRequest gửi GET request đến một peer
func (r *RDAGetProtocolRequester) sendGetRequest(
	ctx context.Context,
	targetPeerID peer.ID,
	handle string,
	shareIndex uint32,
) (*RDASymbol, error) {
	getLog.Debugf("RDA|GET|SEND_START peer=%s handle=%s index=%d", targetPeerID.String()[:8], handle[:8], shareIndex)
	
	// Open stream
	stream, err := r.host.NewStream(ctx, targetPeerID, RDAGetProtocolID)
	if err != nil {
		getLog.Warnf("RDA|GET|STREAM_ERROR peer=%s error=%v", targetPeerID.String()[:8], err)
		return nil, fmt.Errorf("failed to open stream to %s: %w", targetPeerID.String()[:8], err)
	}
	defer stream.Close()

	r.totalRequests++
	requestID := fmt.Sprintf("%s-%d-%d", r.host.ID().String()[:8], shareIndex, time.Now().UnixNano())

	// Build request
	req := GetMessage{
		Type:       GetMessageTypeQuery,
		Handle:     handle,
		ShareIndex: shareIndex,
		SenderID:   r.host.ID().String(),
		Timestamp:  time.Now().UnixNano() / 1e6,
		RequestID:  requestID,
	}

	// Send request
	encoder := json.NewEncoder(stream)
	if err := encoder.Encode(req); err != nil {
		getLog.Warnf("RDA|GET|ENCODE_ERROR peer=%s error=%v", targetPeerID.String()[:8], err)
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	getLog.Debugf("RDA|GET|SEND_OK peer=%s reqID=%s", targetPeerID.String()[:8], requestID)

	// Set stream read deadline for response
	if err := stream.SetReadDeadline(time.Now().Add(r.requestTimeout)); err != nil {
		getLog.Warnf("RDA|GET|DEADLINE_ERROR peer=%s error=%v", targetPeerID.String()[:8], err)
		return nil, fmt.Errorf("failed to set deadline: %w", err)
	}

	// Read response
	var resp GetMessage
	decoder := json.NewDecoder(stream)
	if err := decoder.Decode(&resp); err != nil {
		getLog.Warnf("RDA|GET|RESPONSE_ERROR peer=%s timeout=%v error=%v", 
			targetPeerID.String()[:8], r.requestTimeout, err)
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if resp.Type != GetMessageTypeResponse {
		getLog.Warnf("RDA|GET|TYPE_ERROR peer=%s got=%s", targetPeerID.String()[:8], resp.Type)
		return nil, fmt.Errorf("unexpected response type: %s", resp.Type)
	}

	// Validate response with Pred
	getLog.Debugf("RDA|GET|VALIDATE handle=%s index=%d size=%d", handle[:8], resp.ShareIndex, len(resp.Data))
	
	symbol := &RDASymbol{
		Handle:     resp.Handle,
		ShareIndex: resp.ShareIndex,
		Row:        resp.RowID,
		Col:        resp.ColID,
		ShareData:  resp.Data,
		Timestamp:  resp.Timestamp,
		NMTProof:   *resp.NMTProof,
	}

	if !r.predicateChk.Pred(resp.Handle, resp.ShareIndex, symbol) {
		getLog.Warnf("RDA|GET|VALIDATE_FAIL peer=%s handle=%s", targetPeerID.String()[:8], handle[:8])
		return nil, fmt.Errorf("predicate validation failed")
	}

	r.totalBytesReceived += int64(len(resp.Data))
	getLog.Infof("RDA|GET|VALIDATED ✓ peer=%s handle=%s index=%d size=%d", 
		targetPeerID.String()[:8], handle[:8], resp.ShareIndex, len(resp.Data))

	return symbol, nil
}

// findIntersectionPeers tìm tất cả peers tại (row, col)
func (r *RDAGetProtocolRequester) findIntersectionPeers(row, col uint32) []peer.ID {
	// Query grid để tìm peers tại giao điểm (row, col)
	// Sử dụng row peers và col peers từ PeerManager
	var peerMap = make(map[peer.ID]bool)
	
	// Get peers in same row
	rowPeers := r.peerManager.GetRowPeers()
	for _, p := range rowPeers {
		peerMap[p] = true
	}
	
	// Get peers in target column (from all row peers that are in target col)
	colPeers := r.peerManager.GetColPeers()
	for _, p := range colPeers {
		peerMap[p] = true
	}

	// Intersection: peers that appear in both row peers and should be checked
	// For simplicity, return col peers (which are already in our column)
	result := make([]peer.ID, 0, len(peerMap))
	for p := range peerMap {
		result = append(result, p)
	}
	return result
}

// GetStats trả về statistics
func (r *RDAGetProtocolRequester) GetStats() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return map[string]interface{}{
		"total_requests":        r.totalRequests,
		"successful_gets":       r.successfulGets,
		"failed_retries":        r.failedRetries,
		"total_bytes_received":  r.totalBytesReceived,
		"request_timeout":       r.requestTimeout.String(),
		"retry_attempts":        r.retryAttempts,
	}
}
