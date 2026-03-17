package share

import (
	"context"
	"testing"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRDABlockstoreBridge_ReconstructionCompatibility_EndToEnd
// kích chứng rằng shares lưu via RDA bridge có thể được lấy để reconstruction
// 这是 critical integration test để xác minh fix cho compatibility issue
func TestRDABlockstoreBridge_ReconstructionCompatibility_EndToEnd(t *testing.T) {
	ctx := context.Background()

	// === SETUP ===
	// Tạo blockstore (dùng bởi Celestia Retriever)
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)

	// Tạo RDA storage + bridge
	rdaConfig := RDAStorageConfig{
		MyRow:    0,
		MyCol:    0,
		GridSize: 256,
	}
	rdaStorage := NewRDAStorage(rdaConfig)
	bridge := NewRDAStorageWithBlockstore(rdaStorage, bs)

	// === PHASE 1: STORE SHARES via RDA ===
	// Mô phỏng: RDA STORE protocol nhận shares từ peer
	blockHeight := uint64(100)
	shares := []*RDAShare{
		{Row: 0, Col: 0, SymbolID: 0, Data: []byte("share_0_0___"), Height: blockHeight},
		{Row: 1, Col: 0, SymbolID: 256, Data: []byte("share_1_0___"), Height: blockHeight},
		{Row: 2, Col: 0, SymbolID: 512, Data: []byte("share_2_0___"), Height: blockHeight},
	}

	// Store tất cả shares
	for _, share := range shares {
		err := bridge.StoreShare(ctx, share)
		require.NoError(t, err, "RDA store should succeed for %v", share.SymbolID)
	}

	// === PHASE 2: RETRIEVAL via Celestia's Standard Path ===
	// Mô phỏng: Celestia node calls blockservice.GetBlock (tìm shares trong blockstore)
	// 这 là nơi fix áp dụng - shares phải có trong blockstore!

	for i, originalShare := range shares {
		// Tạo block từ share data (giống cách Retriever searh)
		block := bridge.createIPLDBlock(originalShare)
		require.NotNil(t, block, "Block should be created for share %d", i)

		// === CRITICAL: Lấy block từ blockstore (như Retriever làm) ===
		retrievedBlock, err := bs.Get(ctx, block.Cid())
		require.NoError(t, err, "Share %d should be retrievable from blockstore via blockservice", i)
		require.NotNil(t, retrievedBlock, "Retrieved block should not be nil")

		// Kiểm chứng data khớp
		assert.Equal(t,
			originalShare.Data,
			retrievedBlock.RawData(),
			"Retrieved share %d data should match original",
			i,
		)

		// Kiểm chứng CID khớp (content-addressable guarantee)
		assert.Equal(t,
			block.Cid(),
			retrievedBlock.Cid(),
			"CID should be identical for share %d",
			i,
		)
	}

	// === PHASE 3: VERIFY RECONSTRUCTION PATH ===
	// Kiểm chứng rằng tất cả shares được lưu trong blockstore
	// (đây là điều kiện cần cho Celestia's ExtendedDataSquare.Repair() hoạt động)
	// Đơn giản: verify ta có thể lấy tất cả blocks (implicitly có trong blockstore)

	for _, share := range shares {
		block := bridge.createIPLDBlock(share)
		_, err := bs.Get(ctx, block.Cid())
		require.NoError(t, err, "Share should be in blockstore for reconstruction")
	}

	// Kiểm chứng metrics
	metrics := bridge.GetMetrics()
	assert.Equal(t, int64(len(shares)), metrics["shares_stored_to_blockstore"],
		"All shares should be stored to blockstore")
	assert.Equal(t, int64(0), metrics["blockstore_errors"],
		"Should have no blockstore errors")

	// === PHASE 4: VERIFY ROUND-TRIP ===
	// RDA storage (column-based) + blockstore (for standard retrieval) cùng hoạt động

	for i, originalShare := range shares {
		// Dùng RDA storage để lấy (column-based)
		rdaData, err := bridge.GetShare(ctx, blockHeight, originalShare.Row, originalShare.Col, originalShare.SymbolID)
		require.NoError(t, err, "RDA storage should have share %d", i)
		assert.Equal(t, originalShare.Data, rdaData, "RDA storage data matches")

		// Dùng blockstore để lấy (standard retrieval)
		block := bridge.createIPLDBlock(originalShare)
		blockData, err := bs.Get(ctx, block.Cid())
		require.NoError(t, err, "Blockstore should have share %d", i)
		assert.Equal(t, originalShare.Data, blockData.RawData(), "Blockstore data matches")

		// Khoa are identical
		assert.Equal(t, rdaData, blockData.RawData(),
			"RDA storage and blockstore should have identical data")
	}

	t.Logf("✅ End-to-end reconstruction compatibility test PASSED")
	t.Logf("  - RDA STORE path: ✓ (shares in RDA storage)")
	t.Logf("  - Standard retrieval path: ✓ (shares in blockstore)")
	t.Logf("  - Reconstruction ready: ✓ (both systems have data)")
}

// TestRDABlockstoreBridge_CelestiaReconstructionScenario
// Mô phỏng thực tế kịch bản: Celestia node đang reconstruct sau khi
// nhận shares via RDA network
func TestRDABlockstoreBridge_CelestiaReconstructionScenario(t *testing.T) {
	ctx := context.Background()

	// === SETUP ===
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)

	rdaConfig := RDAStorageConfig{
		MyRow:    5,
		MyCol:    10,
		GridSize: 256,
	}
	rdaStorage := NewRDAStorage(rdaConfig)
	bridge := NewRDAStorageWithBlockstore(rdaStorage, bs)

	// === SCENARIO: Receive shares from RDA peers ===
	// Node này nhận shares sau STORE protocol (vị trí node: row=5, col=10)
	// Nó nhận shares có col=10 từ nhiều rows
	blockHeight := uint64(200)
	incomingShares := []*RDAShare{
		{Row: 0, Col: 10, SymbolID: 10, Data: make([]byte, 1024), Height: blockHeight},   // q từ row 0
		{Row: 1, Col: 10, SymbolID: 266, Data: make([]byte, 1024), Height: blockHeight},  // từ row 1
		{Row: 2, Col: 10, SymbolID: 522, Data: make([]byte, 1024), Height: blockHeight},  // từ row 2
		{Row: 3, Col: 10, SymbolID: 778, Data: make([]byte, 1024), Height: blockHeight},  // từ row 3
		{Row: 4, Col: 10, SymbolID: 1034, Data: make([]byte, 1024), Height: blockHeight}, // từ row 4
		{Row: 5, Col: 10, SymbolID: 1290, Data: make([]byte, 1024), Height: blockHeight}, // chính node (row=5)
		{Row: 6, Col: 10, SymbolID: 1546, Data: make([]byte, 1024), Height: blockHeight}, // từ row 6
	}

	// Fill data để có thể verify integrity
	for i, share := range incomingShares {
		fillBytePattern(share.Data, uint32(i))
	}

	// === RDA STORE HANDLER processes incoming shares ===
	for _, share := range incomingShares {
		err := bridge.StoreShare(ctx, share)
		require.NoError(t, err)
	}

	// === RECONSTRUCTION PATH ===
	// Sau khi shares được store, Celestia node muốn reconstruct EDS
	// Nó sẽ call Retriever.Retrieve() cái sẽ query blockservice/blockstore
	// Kiểm chứng rằng tất cả shares đều accessible

	reconstructionReadyCount := 0
	for _, share := range incomingShares {
		block := bridge.createIPLDBlock(share)
		_, err := bs.Get(ctx, block.Cid())
		if err == nil {
			reconstructionReadyCount++
		}
	}

	assert.Equal(t, len(incomingShares), reconstructionReadyCount,
		"All shares should be ready for reconstruction")

	// === VERIFY DATA INTEGRITY ===
	for i, expectedShare := range incomingShares {
		block := bridge.createIPLDBlock(expectedShare)
		retrievedBlock, err := bs.Get(ctx, block.Cid())
		require.NoError(t, err)

		// Kiểm chứng pattern
		assert.Equal(t, expectedShare.Data, retrievedBlock.RawData(),
			"Data should match for share %d", i)

		verifyBytePattern(t, retrievedBlock.RawData(), uint32(i))
	}

	t.Logf("✅ Celestia reconstruction scenario PASSED")
	t.Logf("  - Shares received via RDA: %d", len(incomingShares))
	t.Logf("  - Shares stored to blockstore: %d", reconstructionReadyCount)
	t.Logf("  - Ready for Retriever.Retrieve() and ExtendedDataSquare.Repair()")
}

// Helper để fill byte pattern (để test data integrity)
func fillBytePattern(data []byte, seed uint32) {
	for i := 0; i < len(data); i++ {
		data[i] = byte((seed + uint32(i)) % 256)
	}
}

// Helper để verify byte pattern
func verifyBytePattern(t *testing.T, data []byte, seed uint32) {
	for i := 0; i < len(data); i++ {
		expected := byte((seed + uint32(i)) % 256)
		assert.Equal(t, expected, data[i], "Byte pattern mismatch at offset %d", i)
	}
}
