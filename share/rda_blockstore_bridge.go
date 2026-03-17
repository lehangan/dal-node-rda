package share

import (
	"context"
	"fmt"
	"sync"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	mh "github.com/multiformats/go-multihash"
)

var rdaBridgeLog = logging.Logger("rda.bridge")

// RDAStorageWithBlockstore bridges RDA storage with Celestia's blockstore
// để đảm bảo Celestia's reconstruction (Reed-Solomon) vẫn hoạt động.
//
// Vấn đề gốc: RDA lưu shares trong in-memory column maps, nhưng Celestia's
// Retriever tìm shares trong blockstore. Bridge này đảm bảo shares lưu trong
// CÁCH thì cũng được lưu trong blockstore để Reconstruction có thể tìm thấy.
//
// Workflow:
//  1. RDA STORE handler gọi bridge.StoreShare(share)
//  2. Bridge lưu vào RDA column structure (cho RDA column sync)
//  3. Bridge CŨNG lưu vào blockstore (cho Celestia reconstruction)
//  4. Khi Retriever.Retrieve() được gọi, shares đã có sẵn trong blockstore
//  5. ExtendedDataSquare.Repair() hoạt động bình thường
type RDAStorageWithBlockstore struct {
	rdaStorage *RDAStorage
	blockstore blockstore.Blockstore
	mu         sync.RWMutex

	// Metrics
	sharesStoredToBlockstore int64
	blockstoreErrors         int64
	blockstoreTotalBytes     int64
}

// NewRDAStorageWithBlockstore tạo bridge kết nối RDAStorage với blockstore
func NewRDAStorageWithBlockstore(
	rdaStorage *RDAStorage,
	bs blockstore.Blockstore,
) *RDAStorageWithBlockstore {
	rdaBridgeLog.Infof(
		"RDA-Blockstore Bridge initialized for node at (row=%d, col=%d, grid=%d)",
		rdaStorage.config.MyRow,
		rdaStorage.config.MyCol,
		rdaStorage.config.GridSize,
	)

	return &RDAStorageWithBlockstore{
		rdaStorage: rdaStorage,
		blockstore: bs,
	}
}

// StoreShare lưu share vào CÁCH RDA column structure VÀ blockstore
// để đảm bảo Celestia's reconstruction có thể tìm thấy
func (b *RDAStorageWithBlockstore) StoreShare(
	ctx context.Context,
	share *RDAShare,
) error {
	rdaBridgeLog.Debugf(
		"RDA-Bridge: StoreShare START height=%d (row=%d, col=%d, symbolID=%d, size=%d bytes)",
		share.Height, share.Row, share.Col, share.SymbolID, len(share.Data),
	)

	// Bước 1: Lưu vào RDA column structure (cho RDA data distribution)
	// RDAStorage.StoreShare() sẽ acquire its own mutex
	if err := b.rdaStorage.StoreShare(ctx, share); err != nil {
		rdaBridgeLog.Warnf(
			"RDA-Bridge: Failed to store in RDA storage: %v",
			err,
		)
		return fmt.Errorf("RDA storage: %w", err)
	}

	// Bước 2: Convert sang IPLD block và lưu vào blockstore
	// (để Celestia's Retriever có thể tìm thấy cho reconstruction)
	block := b.createIPLDBlock(share)
	if block == nil {
		rdaBridgeLog.Warnf(
			"RDA-Bridge: Failed to create IPLD block for symbolID=%d",
			share.SymbolID,
		)
		b.mu.Lock()
		b.blockstoreErrors++
		b.mu.Unlock()
		// NOT returning error - RDA storage succeeded, blockstore is optional
		return nil
	}

	// Put vào blockstore
	if err := b.blockstore.Put(ctx, block); err != nil {
		rdaBridgeLog.Warnf(
			"RDA-Bridge: Failed to put block to blockstore (CID=%s): %v - symbolID=%d",
			block.Cid().String()[:16], err, share.SymbolID,
		)
		b.mu.Lock()
		b.blockstoreErrors++
		b.mu.Unlock()
		// NOT returning error - RDA storage succeeded, blockstore failure is logged but not fatal
	} else {
		rdaBridgeLog.Debugf(
			"RDA-Bridge: Block stored to blockstore - symbolID=%d, CID=%s, size=%d bytes",
			share.SymbolID, block.Cid().String()[:16], len(block.RawData()),
		)
		b.mu.Lock()
		b.sharesStoredToBlockstore++
		b.blockstoreTotalBytes += int64(len(block.RawData()))
		b.mu.Unlock()
	}

	rdaBridgeLog.Debugf(
		"RDA-Bridge: StoreShare SUCCESS - height=%d, (row=%d, col=%d), symbolID=%d ✓",
		share.Height, share.Row, share.Col, share.SymbolID,
	)

	return nil
}

// GetShare truy xuất share từ RDA storage
// (blockstore là read-only cho reconstruction path)
func (b *RDAStorageWithBlockstore) GetShare(
	ctx context.Context,
	height uint64,
	row, col, symbolID uint32,
) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.rdaStorage.GetShare(ctx, height, row, col, symbolID)
}

// RetrieveFromBlockstore cố gắng lấy share từ blockstore
// (Dành cho fallback reconstruction path)
func (b *RDAStorageWithBlockstore) RetrieveFromBlockstore(
	ctx context.Context,
	blockCID cid.Cid,
) ([]byte, error) {
	rdaBridgeLog.Debugf(
		"RDA-Bridge: RetrieveFromBlockstore - CID=%s",
		blockCID.String()[:16],
	)

	block, err := b.blockstore.Get(ctx, blockCID)
	if err != nil {
		rdaBridgeLog.Warnf(
			"RDA-Bridge: Block not found in blockstore (CID=%s): %v",
			blockCID.String()[:16], err,
		)
		return nil, fmt.Errorf("block not found: %w", err)
	}

	rdaBridgeLog.Debugf(
		"RDA-Bridge: Block retrieved from blockstore - CID=%s, size=%d bytes",
		blockCID.String()[:16], len(block.RawData()),
	)

	return block.RawData(), nil
}

// createIPLDBlock converts RDAShare to IPLD block
// Block CID được tính từ multihash của share data (SHA256)
//
// Format CID:
//   - Codec: Raw (0x55)
//   - Hash: SHA256 of share data
//
// Điều này đảm bảo content-addressable storage (tương tự IPLD)
func (b *RDAStorageWithBlockstore) createIPLDBlock(share *RDAShare) blocks.Block {
	// Tính SHA256 multihash của share data
	hash, err := mh.Sum(share.Data, mh.SHA2_256, -1)
	if err != nil {
		rdaBridgeLog.Warnf(
			"RDA-Bridge: Failed to compute multihash - symbolID=%d: %v",
			share.SymbolID, err,
		)
		return nil
	}

	// Tạo CID với:
	// - Version: 1
	// - Codec: Raw (0x55)
	// - Multihash: SHA256
	blockCID := cid.NewCidV1(cid.Raw, hash)

	// Tạo IPLD block
	block, err := blocks.NewBlockWithCid(share.Data, blockCID)
	if err != nil {
		rdaBridgeLog.Warnf(
			"RDA-Bridge: Failed to create block with CID - symbolID=%d: %v",
			share.SymbolID, err,
		)
		return nil
	}

	return block
}

// GetMetrics trả về bridge statistics
func (b *RDAStorageWithBlockstore) GetMetrics() map[string]int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return map[string]int64{
		"shares_stored_to_blockstore": b.sharesStoredToBlockstore,
		"blockstore_errors":           b.blockstoreErrors,
		"blockstore_total_bytes":      b.blockstoreTotalBytes,
	}
}

// VerifySymbolToColumnMapping checks symbol-to-column mapping
func (b *RDAStorageWithBlockstore) VerifySymbolToColumnMapping(symbolID uint32) error {
	return b.rdaStorage.VerifySymbolToColumnMapping(symbolID)
}

// VerifyGridPosition checks if grid position is valid
func (b *RDAStorageWithBlockstore) VerifyGridPosition(row, col uint32) error {
	return b.rdaStorage.VerifyGridPosition(row, col)
}

// ComputeSymbolID converts (row, col) to linear symbolID
func (b *RDAStorageWithBlockstore) ComputeSymbolID(row, col uint32) (uint32, error) {
	return b.rdaStorage.ComputeSymbolID(row, col)
}
