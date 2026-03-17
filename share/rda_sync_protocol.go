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

var syncLog = logging.Logger("rda.sync")

// ============================================================================
// SYNC Protocol Definition - Phase 4: Health Synchronization
// ============================================================================
// Đảm bảo cột không bị mất dữ liệu khi node cũ rời đi, node mới chui vào
// Flow:
//   Phía node mới join (Requester):
//   1. Hook vào startup sau khi discovery xong
//   2. Chờ Δ_sub (delay before pull)
//   3. Gửi (sync) đến láng giềng cùng cột
//   4. Nhận (sync_rsp, S) với mảng shares
//   5. Validate từng shares với Pred → lưu DB
//
//   Phía node cũ trong cột (Responder):
//   1. Listener /rda/sync/1
//   2. Nhận (sync) → scan DB để lấy dữ liệu cùng cột
//   3. Giới hạn theo BlockHeight gần nhất
//   4. Trả (sync_rsp, S) với array

const (
	// /rda/sync/1 - Sync protocol for new nodes joining columns
	RDASyncProtocolID protocol.ID = "/celestia/rda/sync/1.0.0"

	// Max shares per SYNC response to prevent network congestion
	MaxSharesPerSync = 1000

	// Default: scan lấy dữ liệu từ <current_height - MaxBlockHeightWindow> trở lên
	MaxBlockHeightWindow = uint64(60) // ~10 minutes at 10s blocks
)

// SyncMessageType định nghĩa loại thông điệp
type SyncMessageType string

const (
	SyncMessageTypeRequest  SyncMessageType = "sync"     // (sync)
	SyncMessageTypeResponse SyncMessageType = "sync_rsp" // (sync_rsp, S)
)

// SyncMessage - "Phong bè" (envelope) cho SYNC operation
type SyncMessage struct {
	// Message type: "sync" hoặc "sync_rsp"
	Type SyncMessageType `json:"type"`

	// Request-only fields
	RequestCol  uint32 `json:"request_col,omitempty"`   // Cột nào muốn sync
	RequestRow  uint32 `json:"request_row,omitempty"`   // Hàng client đang ở
	SinceHeight uint64 `json:"since_height,omitempty"`  // Chỉ shares từ height này trở lên

	// Response-only fields
	Shares []SyncShareData `json:"shares,omitempty"` // Array S của shares

	// Metadata
	SenderID    string `json:"sender_id"`
	Timestamp   int64  `json:"timestamp"`   // milliseconds
	BlockHeight uint64 `json:"block_height"` // Current block height
	RequestID   string `json:"request_id"`  // To match response with request
}

// SyncShareData - Minimal share data format for SYNC
type SyncShareData struct {
	Handle     string        `json:"handle"`
	ShareIndex uint32        `json:"share_index"`
	Row        uint32        `json:"row"`
	Col        uint32        `json:"col"`
	Data       []byte        `json:"data"`
	Height     uint64        `json:"height"`
	NMTProof   NMTProofData  `json:"nmt_proof"`
}

// ============================================================================
// RDASyncProtocolHandler - Phía trả lời (Node cũ trong cột)
// ============================================================================

type RDASyncProtocolHandler struct {
	host         host.Host
	gridManager  *RDAGridManager
	storage      *RDAStorage // DB cục bộ để scan
	predicateChk *RDAPredicateChecker

	streamHandlers map[string]bool
	mu             sync.RWMutex

	// Statistics
	totalSyncReceived    int64
	totalSyncResponded   int64
	totalSharesSent      int64
	totalBytesTransfer   int64
}

// NewRDASyncProtocolHandler tạo handler mới
func NewRDASyncProtocolHandler(
	host host.Host,
	gridManager *RDAGridManager,
	storage *RDAStorage,
) *RDASyncProtocolHandler {
	gridSize := uint32(gridManager.GetGridDimensions().Cols)
	return &RDASyncProtocolHandler{
		host:               host,
		gridManager:        gridManager,
		storage:            storage,
		predicateChk:       NewRDAPredicateChecker(gridSize),
		streamHandlers:     make(map[string]bool),
		totalSyncReceived:  0,
		totalSyncResponded: 0,
		totalSharesSent:    0,
		totalBytesTransfer: 0,
	}
}

// Start registers stream handlers
func (h *RDASyncProtocolHandler) Start(ctx context.Context) error {
	h.host.SetStreamHandler(RDASyncProtocolID, h.handleSyncStream)

	h.mu.Lock()
	h.streamHandlers[string(RDASyncProtocolID)] = true
	h.mu.Unlock()

	syncLog.Infof("RDA|SYNC|START ✓ Handler started - protocol: %s", RDASyncProtocolID)
	return nil
}

// Stop unregisters stream handlers
func (h *RDASyncProtocolHandler) Stop() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.host.RemoveStreamHandler(RDASyncProtocolID)
	h.streamHandlers[string(RDASyncProtocolID)] = false

	syncLog.Infof("RDA|SYNC|STOP ✓ Handler stopped")
	return nil
}

// handleSyncStream handles incoming SYNC requests from new nodes
func (h *RDASyncProtocolHandler) handleSyncStream(stream network.Stream) {
	defer stream.Close()

	remoteID := stream.Conn().RemotePeer()
	syncLog.Debugf("RDA|SYNC|STREAM_OPEN peer=%s", remoteID.String()[:8])

	// Đọc thông điệp
	var msg SyncMessage
	decoder := json.NewDecoder(stream)
	if err := decoder.Decode(&msg); err != nil {
		syncLog.Warnf("RDA|SYNC|DECODE_ERROR peer=%s error=%v", remoteID.String()[:8], err)
		return
	}

	if msg.Type != SyncMessageTypeRequest {
		syncLog.Warnf("RDA|SYNC|TYPE_ERROR expected=request got=%s from=%s", msg.Type, remoteID.String()[:8])
		return
	}

	h.totalSyncReceived++
	syncLog.Debugf("RDA|SYNC|RECEIVE peer=%s col=%d row=%d sinceHeight=%d",
		remoteID.String()[:8], msg.RequestCol, msg.RequestRow, msg.SinceHeight)

	// ========== BƯỚC 1: SCAN DATABASE ==========
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	shares, err := h.scanColumnShares(ctx, msg.RequestCol, msg.SinceHeight)
	cancel()

	if err != nil {
		syncLog.Warnf("SYNC: Failed to scan column: %v", err)
		return
	}

	if len(shares) == 0 {
		syncLog.Debugf("SYNC: No shares found for col %d", msg.RequestCol)
		// Trả response rỗng
		shares = []SyncShareData{}
	}

	// Giới hạn số shares để tránh packet quá lớn
	if len(shares) > MaxSharesPerSync {
		syncLog.Warnf("SYNC: Truncating %d shares to %d", len(shares), MaxSharesPerSync)
		shares = shares[:MaxSharesPerSync]
	}

	// ========== BƯỚC 2: ASSEMBLE & SEND RESPONSE ==========
	response := SyncMessage{
		Type:        SyncMessageTypeResponse,
		Shares:      shares,
		SenderID:    h.host.ID().String(),
		Timestamp:   time.Now().UnixNano() / 1e6,
		BlockHeight: 0, // Placeholder
		RequestID:   msg.RequestID,
	}

	respData, err := json.Marshal(response)
	if err != nil {
		syncLog.Warnf("SYNC: Failed to marshal response: %v", err)
		return
	}

	if _, err := stream.Write(respData); err != nil {
		syncLog.Warnf("SYNC: Failed to send response: %v", err)
		return
	}

	h.totalSyncResponded++
	h.totalSharesSent += int64(len(shares))
	h.totalBytesTransfer += int64(len(respData))

	syncLog.Infof("SYNC SUCCESS ✓ - sent %d shares to %s (size=%d bytes)",
		len(shares), remoteID.String()[:8], len(respData))
}

// scanColumnShares scans local DB for shares in a specific column
// Lấy dữ liệu từ BlockHeight gần đây (tối đa MaxBlockHeightWindow)\
func (h *RDASyncProtocolHandler) scanColumnShares(
	ctx context.Context,
	col uint32,
	sinceHeight uint64,
) ([]SyncShareData, error) {
	// Placeholder implementation - trong thực tế scan từ RDAStorage
	// For now, return empty list (Storage cần implement query interface riêng)
	result := []SyncShareData{}

	// TODO: Implement actual DB scan when RDAStorage adds query API
	// Should:
	// 1. Iterate through stored shares
	// 2. Filter: share.Col == col AND share.Height >= sinceHeight
	// 3. Limit by MaxSharesPerSync

	return result, nil
}

// GetStats trả về statistics
func (h *RDASyncProtocolHandler) GetStats() map[string]interface{} {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return map[string]interface{}{
		"total_sync_received":  h.totalSyncReceived,
		"total_sync_responded": h.totalSyncResponded,
		"total_shares_sent":    h.totalSharesSent,
		"total_bytes_transfer": h.totalBytesTransfer,
	}
}

// ============================================================================
// RDASyncProtocolRequester - Phía hỏi (Node mới join)
// ============================================================================

type RDASyncProtocolRequester struct {
	host            host.Host
	gridManager     *RDAGridManager
	peerManager     *RDAPeerManager
	storage         *RDAStorage
	predicateChk    *RDAPredicateChecker
	subnetMgr       *RDASubnetManager

	mu                  sync.RWMutex
	syncDelay           time.Duration
	requestTimeout      time.Duration
	totalSyncAttempts   int64
	successfulSyncs     int64
	failedSyncs         int64
	totalSharesReceived int64
	totalBytesReceived  int64

	// To prevent multiple syncs
	syncedOnce bool
	syncOnce   sync.Once
}

// NewRDASyncProtocolRequester tạo requester mới
func NewRDASyncProtocolRequester(
	host host.Host,
	gridManager *RDAGridManager,
	peerManager *RDAPeerManager,
	storage *RDAStorage,
	subnetMgr *RDASubnetManager,
	syncDelay time.Duration,
) *RDASyncProtocolRequester {
	if syncDelay == 0 {
		syncDelay = 4 * time.Second // Default: ~4 rounds
	}
	gridSize := uint32(gridManager.GetGridDimensions().Cols)
	return &RDASyncProtocolRequester{
		host:                host,
		gridManager:         gridManager,
		peerManager:         peerManager,
		storage:             storage,
		predicateChk:        NewRDAPredicateChecker(gridSize),
		subnetMgr:           subnetMgr,
		syncDelay:           syncDelay,
		requestTimeout:      10 * time.Second,
		totalSyncAttempts:   0,
		successfulSyncs:     0,
		failedSyncs:         0,
		totalSharesReceived: 0,
		totalBytesReceived:  0,
		syncedOnce:          false,
	}
}

// TriggerSyncOnStartup should be called in RDANodeService.Start() after discovery
// Hooks vào startup process: sau khi Tầng Khám phá kết nối xong
func (r *RDASyncProtocolRequester) TriggerSyncOnStartup(ctx context.Context) {
	r.syncOnce.Do(func() {
		// Chờ Δ_sub (delay before pull)
		timer := time.NewTimer(r.syncDelay)
		defer timer.Stop()

		select {
		case <-timer.C:
			// Thực hiện sync
			go r.performColumnSync(ctx)
		case <-ctx.Done():
			syncLog.Debugf("SYNC startup cancelled before delay")
		}
	})
}

// performColumnSync performs the actual sync operation
func (r *RDASyncProtocolRequester) performColumnSync(ctx context.Context) {
	myPos, found := r.gridManager.GetPeerPosition(r.host.ID())
	if !found {
		syncLog.Warnf("SYNC: Cannot determine my position")
		r.failedSyncs++
		return
	}

	myCol := uint32(myPos.Col)
	myRow := uint32(myPos.Row)

	syncLog.Infof("SYNC: Starting column sync - myRow=%d, myCol=%d", myRow, myCol)

	// Tìm láng giềng trong cùng cột
	colPeers := r.findColumnPeers(myCol)
	if len(colPeers) == 0 {
		syncLog.Warnf("SYNC: No column peers found for col %d", myCol)
		r.failedSyncs++
		return
	}

	syncLog.Debugf("SYNC: Found %d column peers", len(colPeers))

	r.totalSyncAttempts++

	// Try sync from peers (retry logic)
	for attempt := 0; attempt < 3; attempt++ {
		for _, peer := range colPeers {
			err := r.syncFromPeer(ctx, peer, myCol, myRow)
			if err == nil {
				r.successfulSyncs++
				r.syncedOnce = true
				syncLog.Infof("SYNC SUCCESS ✓ - synced from %s (attempt %d)",
					peer.String()[:8], attempt+1)
				return
			}
			syncLog.Debugf("SYNC: Peer %s sync failed (attempt %d): %v",
				peer.String()[:8], attempt+1, err)
		}
	}

	r.failedSyncs++
	syncLog.Warnf("SYNC FAILED - could not sync from any column peer")
}

// syncFromPeer performs sync from a specific peer
func (r *RDASyncProtocolRequester) syncFromPeer(
	ctx context.Context,
	peerID peer.ID,
	myCol uint32,
	myRow uint32,
) error {
	// Open stream
	stream, err := r.host.NewStream(ctx, peerID, RDASyncProtocolID)
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	syncLog.Debugf("SYNC: Opened stream to %s for col %d", peerID.String()[:8], myCol)

	// Build request
	req := SyncMessage{
		Type:        SyncMessageTypeRequest,
		RequestCol:  myCol,
		RequestRow:  myRow,
		SinceHeight: 0, // Get all data (could be optimized)
		SenderID:    r.host.ID().String(),
		Timestamp:   time.Now().UnixNano() / 1e6,
		RequestID:   fmt.Sprintf("%s-sync-%d", r.host.ID().String()[:8], time.Now().UnixNano()),
	}

	// Send request
	encoder := json.NewEncoder(stream)
	if encErr := encoder.Encode(req); encErr != nil {
		return fmt.Errorf("failed to send request: %w", encErr)
	}

	// Set stream read deadline
	if err := stream.SetReadDeadline(time.Now().Add(r.requestTimeout)); err != nil {
		return fmt.Errorf("failed to set deadline: %w", err)
	}

	// Read response
	var resp SyncMessage
	decoder := json.NewDecoder(stream)
	if decodeErr := decoder.Decode(&resp); decodeErr != nil {
		return fmt.Errorf("failed to decode response: %w", decodeErr)
	}

	if resp.Type != SyncMessageTypeResponse {
		return fmt.Errorf("unexpected response type: %s", resp.Type)
	}

	// ========== BƯỚC 4: VALIDATE VÀ LƯU TỪNG SHARE ==========
	storeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	validCount := 0
	for _, shareData := range resp.Shares {
		symbol := &RDASymbol{
			Handle:     shareData.Handle,
			ShareIndex: shareData.ShareIndex,
			Row:        shareData.Row,
			Col:        shareData.Col,
			ShareData:  shareData.Data,
			Timestamp:  int64(shareData.Height),
			NMTProof:   shareData.NMTProof,
		}

		// Validate with Pred
		if !r.predicateChk.Pred(shareData.Handle, shareData.ShareIndex, symbol) {
			syncLog.Debugf("SYNC: Predicate validation failed for (h=%s, i=%d)",
				shareData.Handle[:8], shareData.ShareIndex)
			continue
		}

		// Store locally
		rdaShare := &RDAShare{
			Row:      shareData.Row,
			Col:      shareData.Col,
			SymbolID: shareData.ShareIndex,
			Data:     shareData.Data,
			Height:   shareData.Height,
		}

		if err := r.storage.StoreShare(storeCtx, rdaShare); err != nil {
			syncLog.Warnf("SYNC: Failed to store share: %v", err)
			continue
		}

		validCount++
		r.totalSharesReceived++
	}

	r.totalBytesReceived += int64(len(resp.Shares))
	syncLog.Infof("SYNC STORED ✓ - %d/%d shares validated and saved from %s",
		validCount, len(resp.Shares), peerID.String()[:8])

	return nil
}

// findColumnPeers tìm tất cả peers trong cùng cột
func (r *RDASyncProtocolRequester) findColumnPeers(col uint32) []peer.ID {
	// Query PeerManager để tìm peers ở cùng cột
	return r.peerManager.GetColPeers()
}

// IsSynced returns whether this node has completed initial sync
func (r *RDASyncProtocolRequester) IsSynced() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.syncedOnce
}

// GetStats trả về statistics
func (r *RDASyncProtocolRequester) GetStats() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return map[string]interface{}{
		"total_sync_attempts":   r.totalSyncAttempts,
		"successful_syncs":      r.successfulSyncs,
		"failed_syncs":          r.failedSyncs,
		"total_shares_received": r.totalSharesReceived,
		"total_bytes_received":  r.totalBytesReceived,
		"synced":                r.syncedOnce,
		"sync_delay":            r.syncDelay.String(),
	}
}
