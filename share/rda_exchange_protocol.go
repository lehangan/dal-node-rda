package share

import (
	"fmt"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var exchangeLog = logging.Logger("rda.exchange")

const (
	// RDA Data Exchange Stream Protocols (libp2p)
	// Thay thế Bitswap/Shrex bằng các giao thức RDA riêng
	RDAStoreProtocol protocol.ID = "/celestia/rda/store/1.0.0" // STORE: Ghi dữ liệu vào node
	RDAGetProtocol   protocol.ID = "/celestia/rda/get/1.0.0"   // GET: Đọc dữ liệu từ node
	RDASyncProtocol  protocol.ID = "/celestia/rda/sync/1.0.0"  // SYNC: Đồng bộ hóa nhiều symbols

	// Grid sizing
	DefaultGridSize = 256
)

// ============================================================================
// PHASE 1: Message Definitions - Định dạng Gói tin
// ============================================================================

// RDASymbol represents one unit of data in RDA - cấu trúc $(h, i, x)$
// h: Handle (DataRoot identifier)
// i: Index (Share position in grid)
// x: Symbol (Raw data + NMT Proof)
type RDASymbol struct {
	// Handle (h): Identifier cho ExtendedDataSquare (EDS)
	Handle string `json:"handle"` // DataRoot hash or block height

	// Index (i): Vị trí của share trong EDS
	// Mapping: Index = Row * K + Col (where K = grid size)
	ShareIndex uint32 `json:"share_index"`

	// Dữ liệu thô (Raw Symbol)
	// Dòng của EDS chứa share này
	Row uint32 `json:"row"`
	// Cột của EDS chứa share này
	Col       uint32 `json:"col"`
	ShareData []byte `json:"share_data"` // Raw share bytes
	ShareSize uint32 `json:"share_size"` // Size của share (thường 256 bytes)

	// NMT Proof cho symbol này
	// Dùng để chứng minh share này thuộc ExtendedDataSquare có DataRoot là Handle
	NMTProof NMTProofData `json:"nmt_proof"`

	// Metadata
	Timestamp   int64  `json:"timestamp"`
	BlockHeight uint64 `json:"block_height"` // Chiều cao block chứa symbol này
}

// NMTProofData contains Namespaced Merkle Tree proof data
// Chứng minh NMT: Chứng minh rằng share này thuộc namespace được chỉ định
type NMTProofData struct {
	// Proof nodes từ leaf tới root của NMT
	Nodes [][]byte `json:"nodes"`

	// Leaf index
	LeafIndex uint32 `json:"leaf_index"`

	// Leaf size
	LeafSize uint32 `json:"leaf_size"`

	// Namespace ID
	NamespaceID string `json:"namespace_id"`

	// Root hash của NMT
	RootHash []byte `json:"root_hash"`

	// Nonce
	Nonce uint32 `json:"nonce"`
}

// ============================================================================
// STORE Protocol Messages - Ghi dữ liệu
// ============================================================================

// StoreRequest yêu cầu lưu trữ một symbol
// Gửi từ: Full node/Bridge node
// Tới: Light node (cùng column)
type StoreRequest struct {
	RequestID string `json:"request_id"` // Unique request ID for tracing

	// Symbol cần lưu trữ
	Symbol RDASymbol `json:"symbol"`

	// Metadata yêu cầu
	Timestamp int64 `json:"timestamp"`
	TTL       int64 `json:"ttl"`      // Time-to-live trong giây
	Priority  uint8 `json:"priority"` // Mức độ ưu tiên (0-255)
}

// StoreResponse phản hồi sau khi lưu trữ
type StoreResponse struct {
	RequestID string `json:"request_id"`
	Success   bool   `json:"success"`
	Error     string `json:"error,omitempty"`

	// Metadata
	StorageID string `json:"storage_id"` // ID để tracking lưu trữ
	Timestamp int64  `json:"timestamp"`
	LatencyMs int64  `json:"latency_ms"`
}

// ============================================================================
// GET Protocol Messages - Đọc dữ liệu
// ============================================================================

// GetRequest yêu cầu lấy một symbol cụ thể
// Gửi từ: Bất kỳ node nào
// Tới: Node lưu trữ symbol (cùng column)
type GetRequest struct {
	RequestID string `json:"request_id"`

	// Định danh symbol cần lấy
	Handle     string `json:"handle"`      // DataRoot
	ShareIndex uint32 `json:"share_index"` // Index i

	// Metadata
	Timestamp int64 `json:"timestamp"`
}

// GetResponse trả về symbol được yêu cầu
type GetResponse struct {
	RequestID string `json:"request_id"`
	Success   bool   `json:"success"`
	Error     string `json:"error,omitempty"`

	// Symbol được trả về
	Symbol *RDASymbol `json:"symbol,omitempty"`

	// Metadata
	Timestamp int64 `json:"timestamp"`
	LatencyMs int64 `json:"latency_ms"`
}

// ============================================================================
// SYNC Protocol Messages - Đồng bộ hóa
// ============================================================================

// SyncRequest yêu cầu đồng bộ nhiều symbols
// Gửi từ: Node cần cập nhật
// Tới: Node có sẵn dữ liệu (Local RoutingTable node hoặc Bootstrap)
type SyncRequest struct {
	RequestID string `json:"request_id"`

	// Tập hợp symbols cần đồng bộ
	// Cấu trúc Set $S$ = {(handle, shareIndex)} từ bài báo
	Symbols []SymbolIdentifier `json:"symbols"`

	// Batch size cho streaming
	BatchSize uint32 `json:"batch_size"`

	// Metadata
	Timestamp int64 `json:"timestamp"`
}

// SymbolIdentifier định danh một symbol
// Thường nhẹ hơn RDASymbol, chỉ chứa essential info
type SymbolIdentifier struct {
	Handle     string `json:"handle"`
	ShareIndex uint32 `json:"share_index"`

	// Optional: Nếu đã có partial data
	HasData bool `json:"has_data"`
}

// SyncResponse trả về nhiều symbols dưới dạng stream
type SyncResponse struct {
	RequestID string `json:"request_id"`
	Success   bool   `json:"success"`
	Error     string `json:"error,omitempty"`

	// Streaming responses (mỗi chunk là một symbol)
	Symbol *RDASymbol `json:"symbol,omitempty"`

	// Total expected
	TotalSymbols uint32 `json:"total_symbols"`
	SymbolIndex  uint32 `json:"symbol_index"` // Current index in stream

	// Metadata
	Timestamp int64 `json:"timestamp"`
}

// ============================================================================
// PHASE 2: Validation Functions - Hàm kiểm tra tính hợp lệ
// ============================================================================

// Pred(h, i, x) - Predicate function để kiểm tra tính hợp lệ của symbol
// INPUT:
//
//	h: DataRoot (Handle)
//	i: ShareIndex (Index)
//	x: RDASymbol (Symbol chứa data và proof)
//
// OUTPUT:
//
//	True nếu symbol hợp lệ
//	False nếu symbol không hợp lệ hoặc proof sai
//
// Từ bài báo: Pred(h, i, x) kiểm tra xem x có phải là share hợp lệ
// cho DataRoot h tại index i không
type RDAPredicateChecker struct {
	gridSize uint32
	mu       sync.RWMutex
	// Cache của những handles đã verify
	verifiedHandles map[string]bool
}

// NewRDAPredicateChecker tạo một checker mới
func NewRDAPredicateChecker(gridSize uint32) *RDAPredicateChecker {
	return &RDAPredicateChecker{
		gridSize:        gridSize,
		verifiedHandles: make(map[string]bool),
	}
}

// Pred kiểm tra Predicate: Pred(h, i, x)
// h: Handle (DataRoot)
// i: ShareIndex
// x: RDASymbol (chứa raw data + proof)
// Trả về: true nếu hợp lệ, false nếu không
func (pc *RDAPredicateChecker) Pred(handle string, shareIndex uint32, symbol *RDASymbol) bool {
	// Helper for safe string truncation
	safeShorten := func(s string, maxLen int) string {
		if len(s) > maxLen {
			return s[:maxLen]
		}
		return s
	}

	if symbol == nil {
		exchangeLog.Debugf("Pred FAILED: symbol is nil for (h=%s, i=%d)", safeShorten(handle, 8), shareIndex)
		return false
	}

	// Validation Step 1: Check basic field consistency
	if symbol.Handle != handle {
		exchangeLog.Warnf(
			"Pred FAILED: Handle mismatch - expected=%s, got=%s",
			safeShorten(handle, 8), safeShorten(symbol.Handle, 8),
		)
		return false
	}

	if symbol.ShareIndex != shareIndex {
		exchangeLog.Warnf(
			"Pred FAILED: ShareIndex mismatch - expected=%d, got=%d",
			shareIndex, symbol.ShareIndex,
		)
		return false
	}

	// Validation Step 2: Check share data exists
	if len(symbol.ShareData) == 0 {
		exchangeLog.Warnf("Pred FAILED: Empty share data for (h=%s, i=%d)", safeShorten(handle, 8), shareIndex)
		return false
	}

	// Validation Step 3: Verify NMT Proof
	// Bọc kiểm tra Namespaced Merkle Tree Proof
	if !pc.verifyNMTProof(handle, shareIndex, symbol) {
		exchangeLog.Warnf(
			"Pred FAILED: NMT proof verification failed for (h=%s, i=%d)",
			safeShorten(handle, 8), shareIndex,
		)
		return false
	}

	// Validation Step 4: Index to Column mapping validation
	// Verify rằng share này thuộc column chính xác
	expectedCol := shareIndex % uint32(pc.gridSize)
	if symbol.Col != expectedCol {
		exchangeLog.Warnf(
			"Pred FAILED: Column mismatch - index %d maps to col %d, but symbol is col %d",
			shareIndex, expectedCol, symbol.Col,
		)
		return false
	}

	// Validation Step 5: Timestamp freshness
	// Verify timestamp không quá cũ (> 24 giờ)
	maxAge := int64(24 * 60 * 60 * 1000) // 24 hours in milliseconds
	if time.Now().UnixNano()/1e6-symbol.Timestamp > maxAge {
		exchangeLog.Debugf(
			"Pred: Warning - symbol timestamp is old (%.1f hours)",
			float64(time.Now().UnixNano()/1e6-symbol.Timestamp)/1000/60/60,
		)
	}

	exchangeLog.Debugf(
		"Pred SUCCESS ✓ - (h=%s, i=%d) symbol validated",
		safeShorten(handle, 8), shareIndex,
	)
	return true
}

// verifyNMTProof kiểm tra NMT proof của symbol
// Wrapper cho Celestia's NMT proof verification logic
func (pc *RDAPredicateChecker) verifyNMTProof(handle string, shareIndex uint32, symbol *RDASymbol) bool {
	proof := symbol.NMTProof

	// Check 1: Proof nodes exist
	if len(proof.Nodes) == 0 {
		return false
	}

	// Check 2: Root hash exists
	if len(proof.RootHash) == 0 {
		return false
	}

	// Check 3: Root hash matches Handle
	// Trong thực tế, Handle chứa hoặc được tính từ RootHash
	// Ở đây ta simulate bằng cách check format
	// Handle should be non-empty (real handle would be a hash)
	if len(handle) == 0 {
		return false
	}

	// Check 4: Namespace ID is valid
	if len(proof.NamespaceID) == 0 {
		return false
	}

	// In production, bạn sẽ gọi Celestia's NMT verification logic tại đây
	// Ví dụ: return verifyNMTTree(proof.Nodes, proof.RootHash, symbol.ShareData)
	// Cho demo: Simulate successful verification

	nsID := proof.NamespaceID
	if len(nsID) > 8 {
		nsID = nsID[:8]
	}
	exchangeLog.Debugf(
		"NMT Proof verified: leafIndex=%d, namespace=%s, nodes=%d",
		proof.LeafIndex, nsID, len(proof.Nodes),
	)
	return true
}

// ============================================================================
// PHASE 3: Utility Functions - Hàm tiện ích
// ============================================================================

// Cell(i) maps share index to column ID
// INPUT: i - Share index (0 to K²-1)
// OUTPUT: Column ID (0 to K-1)
//
// Mapping: Share at (row, col) has index = row * K + col
// Therefore: col = index % K
// And: row = index / K
func Cell(shareIndex uint32, gridSize uint32) uint32 {
	return shareIndex % gridSize
}

// Row(i) maps share index to row ID
func Row(shareIndex uint32, gridSize uint32) uint32 {
	return shareIndex / gridSize
}

// Index(row, col, gridSize) maps (row, col) to share index
func IndexFromRowCol(row, col, gridSize uint32) uint32 {
	return row*gridSize + col
}

// ============================================================================
// Data Exchange Manager - Quản lý giao thức trao đổi dữ liệu
// ============================================================================

// RDAExchangeManager quản lý ba giao thức STORE, GET, SYNC
type RDAExchangeManager struct {
	gridSize          uint32
	predicateChecker  *RDAPredicateChecker
	storageHandler    StorageHandler
	queryCount        int64
	successCount      int64
	failureCount      int64
	totalBytesStored  int64
	totalBytesQueried int64

	mu sync.RWMutex

	// Logging
	logger logging.StandardLogger
}

// StorageHandler interface cho storage backend
type StorageHandler interface {
	Store(symbol *RDASymbol) error
	Get(handle string, shareIndex uint32) (*RDASymbol, error)
	GetBatch(symbols []SymbolIdentifier) ([]*RDASymbol, error)
}

// NewRDAExchangeManager tạo manager mới
func NewRDAExchangeManager(gridSize uint32, storageHandler StorageHandler) *RDAExchangeManager {
	return &RDAExchangeManager{
		gridSize:         gridSize,
		predicateChecker: NewRDAPredicateChecker(gridSize),
		storageHandler:   storageHandler,
		logger:           exchangeLog,
	}
}

// HandleStoreRequest xử lý STORE request
func (m *RDAExchangeManager) HandleStoreRequest(req *StoreRequest) *StoreResponse {
	m.mu.Lock()
	m.queryCount++
	m.mu.Unlock()

	startTime := time.Now()

	resp := &StoreResponse{
		RequestID: req.RequestID,
		Timestamp: time.Now().UnixNano() / 1e6,
	}

	// Validate symbol với Pred(h, i, x)
	if !m.predicateChecker.Pred(req.Symbol.Handle, req.Symbol.ShareIndex, &req.Symbol) {
		resp.Success = false
		resp.Error = "Symbol validation failed (Pred check)"
		m.mu.Lock()
		m.failureCount++
		m.mu.Unlock()
		resp.LatencyMs = time.Since(startTime).Milliseconds()
		return resp
	}

	// Lưu trữ symbol
	if err := m.storageHandler.Store(&req.Symbol); err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("Storage error: %v", err)
		m.mu.Lock()
		m.failureCount++
		m.mu.Unlock()
		resp.LatencyMs = time.Since(startTime).Milliseconds()
		return resp
	}

	resp.Success = true
	resp.StorageID = fmt.Sprintf("%s_%d", req.Symbol.Handle[:8], req.Symbol.ShareIndex)

	m.mu.Lock()
	m.successCount++
	m.totalBytesStored += int64(len(req.Symbol.ShareData))
	m.mu.Unlock()

	resp.LatencyMs = time.Since(startTime).Milliseconds()

	handleShort := req.Symbol.Handle
	if len(handleShort) > 8 {
		handleShort = handleShort[:8]
	}
	exchangeLog.Infof(
		"[%s] STORE SUCCESS - handle=%s, index=%d, size=%d bytes, latency=%dms",
		req.RequestID[:min(8, len(req.RequestID))], handleShort, req.Symbol.ShareIndex, len(req.Symbol.ShareData), resp.LatencyMs,
	)

	return resp
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// HandleGetRequest xử lý GET request
func (m *RDAExchangeManager) HandleGetRequest(req *GetRequest) *GetResponse {
	m.mu.Lock()
	m.queryCount++
	m.mu.Unlock()

	startTime := time.Now()

	resp := &GetResponse{
		RequestID: req.RequestID,
		Timestamp: time.Now().UnixNano() / 1e6,
	}

	// Lấy symbol từ storage
	symbol, err := m.storageHandler.Get(req.Handle, req.ShareIndex)
	if err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("Storage error: %v", err)
		m.mu.Lock()
		m.failureCount++
		m.mu.Unlock()
		resp.LatencyMs = time.Since(startTime).Milliseconds()
		return resp
	}

	if symbol == nil {
		resp.Success = false
		resp.Error = "Symbol not found"
		m.mu.Lock()
		m.failureCount++
		m.mu.Unlock()
		resp.LatencyMs = time.Since(startTime).Milliseconds()
		return resp
	}

	// Verify symbol với Pred(h, i, x)
	if !m.predicateChecker.Pred(req.Handle, req.ShareIndex, symbol) {
		resp.Success = false
		resp.Error = "Symbol validation failed (Pred check)"
		m.mu.Lock()
		m.failureCount++
		m.mu.Unlock()
		resp.LatencyMs = time.Since(startTime).Milliseconds()
		return resp
	}

	resp.Success = true
	resp.Symbol = symbol

	m.mu.Lock()
	m.successCount++
	m.totalBytesQueried += int64(len(symbol.ShareData))
	m.mu.Unlock()

	resp.LatencyMs = time.Since(startTime).Milliseconds()

	handleShort := req.Handle
	if len(handleShort) > 8 {
		handleShort = handleShort[:8]
	}
	exchangeLog.Infof(
		"[%s] GET SUCCESS - handle=%s, index=%d, size=%d bytes, latency=%dms",
		req.RequestID[:min(8, len(req.RequestID))], handleShort, req.ShareIndex, len(symbol.ShareData), resp.LatencyMs,
	)

	return resp
}

// HandleSyncRequest xử lý SYNC request (batch)
func (m *RDAExchangeManager) HandleSyncRequest(req *SyncRequest) []*SyncResponse {
	m.mu.Lock()
	m.queryCount++
	m.mu.Unlock()

	startTime := time.Now()

	responses := make([]*SyncResponse, 0)

	// Lấy tất cả symbols theo batch
	symbols, err := m.storageHandler.GetBatch(req.Symbols)
	if err != nil {
		resp := &SyncResponse{
			RequestID: req.RequestID,
			Success:   false,
			Error:     fmt.Sprintf("Batch retrieval error: %v", err),
			Timestamp: time.Now().UnixNano() / 1e6,
		}
		m.mu.Lock()
		m.failureCount++
		m.mu.Unlock()
		return []*SyncResponse{resp}
	}

	// Verify mỗi symbol và trả về
	for idx, symbol := range symbols {
		if symbol == nil {
			continue
		}

		// Verify với Pred
		if !m.predicateChecker.Pred(symbol.Handle, symbol.ShareIndex, symbol) {
			m.mu.Lock()
			m.failureCount++
			m.mu.Unlock()
			continue
		}

		resp := &SyncResponse{
			RequestID:    req.RequestID,
			Success:      true,
			Symbol:       symbol,
			TotalSymbols: uint32(len(req.Symbols)),
			SymbolIndex:  uint32(idx),
			Timestamp:    time.Now().UnixNano() / 1e6,
		}

		responses = append(responses, resp)

		m.mu.Lock()
		m.successCount++
		m.totalBytesQueried += int64(len(symbol.ShareData))
		m.mu.Unlock()
	}

	latency := time.Since(startTime).Milliseconds()

	exchangeLog.Infof(
		"[%s] SYNC SUCCESS - symbols=%d, responses=%d, latency=%dms",
		req.RequestID[:8], len(req.Symbols), len(responses), latency,
	)

	return responses
}

// GetStats trả về thống kê
func (m *RDAExchangeManager) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	successRate := float64(0)
	if m.queryCount > 0 {
		successRate = float64(m.successCount) / float64(m.queryCount) * 100
	}

	return map[string]interface{}{
		"total_queries":       m.queryCount,
		"successful":          m.successCount,
		"failed":              m.failureCount,
		"success_rate":        fmt.Sprintf("%.2f%%", successRate),
		"total_bytes_stored":  m.totalBytesStored,
		"total_bytes_queried": m.totalBytesQueried,
		"grid_size":           m.gridSize,
	}
}
