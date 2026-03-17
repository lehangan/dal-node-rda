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

// TestRDAStorageWithBlockstore_StoreAndRetrieve kiểm chứng StoreShare
// lưu vào CÁCH RDA storage VÀ blockstore
func TestRDAStorageWithBlockstore_StoreAndRetrieve(t *testing.T) {
	ctx := context.Background()

	// Tạo blockstore
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)

	// Tạo RDA storage
	rdaConfig := RDAStorageConfig{
		MyRow:    0,
		MyCol:    0,
		GridSize: 256,
	}
	rdaStorage := NewRDAStorage(rdaConfig)

	// Tạo bridge
	bridge := NewRDAStorageWithBlockstore(rdaStorage, bs)

	// Tạo test share (column 0, row 1)
	testShare := &RDAShare{
		Row:      1,   // row 1
		Col:      0,   // column 0 (node's column)
		SymbolID: 256, // symbolID = row * K + col = 1 * 256 + 0
		Data:     []byte("test share data"),
		Height:   100,
	}

	// Lưu share qua bridge
	err := bridge.StoreShare(ctx, testShare)
	require.NoError(t, err, "StoreShare should succeed")

	// Kiểm chứng share được lưu trong RDA storage
	retrievedData, err := bridge.GetShare(ctx, 100, 1, 0, 256)
	require.NoError(t, err, "GetShare should find the share")
	assert.Equal(t, testShare.Data, retrievedData, "Retrieved data should match stored data")

	// Kiểm chứng share được lưu trong blockstore
	blockCID := bridge.createIPLDBlock(testShare).Cid()
	block, err := bs.Get(ctx, blockCID)
	require.NoError(t, err, "Block should be found in blockstore")
	assert.Equal(t, testShare.Data, block.RawData(), "Block data should match share data")

	// Kiểm chứng metrics
	metrics := bridge.GetMetrics()
	assert.Equal(t, int64(1), metrics["shares_stored_to_blockstore"], "Should have 1 share in blockstore")
	assert.Equal(t, int64(0), metrics["blockstore_errors"], "Should have no blockstore errors")
}

// TestRDAStorageWithBlockstore_BlockstoreRetrievalForReconstruction
// kiểm chứng rằng shares có thể được lấy từ blockstore (cho reconstruction)
func TestRDAStorageWithBlockstore_BlockstoreRetrievalForReconstruction(t *testing.T) {
	ctx := context.Background()

	// Tạo blockstore
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)

	// Tạo RDA storage + bridge
	rdaConfig := RDAStorageConfig{
		MyRow:    0,
		MyCol:    1,
		GridSize: 256,
	}
	rdaStorage := NewRDAStorage(rdaConfig)
	bridge := NewRDAStorageWithBlockstore(rdaStorage, bs)

	// Lưu nhiều shares (cũng như Celestia store thực tế)
	shares := []*RDAShare{
		{
			Row:      0,
			Col:      1,
			SymbolID: 1, // 0 * 256 + 1
			Data:     []byte("share_0_1"),
			Height:   100,
		},
		{
			Row:      5,
			Col:      1,
			SymbolID: 1281, // 5 * 256 + 1
			Data:     []byte("share_5_1"),
			Height:   100,
		},
	}

	for _, share := range shares {
		err := bridge.StoreShare(ctx, share)
		require.NoError(t, err)
	}

	// Kiểm chứng tất cả shares được lưu trong blockstore
	for _, share := range shares {
		block := bridge.createIPLDBlock(share)
		blockFromStore, err := bs.Get(ctx, block.Cid())
		require.NoError(t, err, "Share should be retrievable from blockstore: %v", share.SymbolID)
		assert.Equal(t, share.Data, blockFromStore.RawData())
	}

	// Kiểm chứng RetrieveFromBlockstore hoạt động
	for _, share := range shares {
		block := bridge.createIPLDBlock(share)
		blockCID := block.Cid()
		data, err := bridge.RetrieveFromBlockstore(ctx, blockCID)
		require.NoError(t, err)
		assert.Equal(t, share.Data, data)
	}
}

// TestRDAStorageWithBlockstore_ColumnAffinityRespected
// kiểm chứng RDA column affinity rules được tuân thủ even khi lưu to blockstore
func TestRDAStorageWithBlockstore_ColumnAffinityRespected(t *testing.T) {
	ctx := context.Background()

	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)

	rdaConfig := RDAStorageConfig{
		MyRow:    0,
		MyCol:    2,
		GridSize: 256,
	}
	rdaStorage := NewRDAStorage(rdaConfig)
	bridge := NewRDAStorageWithBlockstore(rdaStorage, bs)

	// Test case 1: Share với symbolID không map tới node's column
	// symbolID = 3 => column = 3 % 256 = 3, nhưng node ở column 2
	invalidShare := &RDAShare{
		Row:      0,
		Col:      2, // Tập hợp data column này nhưng...
		SymbolID: 3, // .. symbolID map tới column 3 (CONFLICT!)
		Data:     []byte("invalid"),
		Height:   100,
	}

	err := bridge.StoreShare(ctx, invalidShare)
	// RDA storage should reject this
	assert.Error(t, err, "Should reject share với column affinity violation")

	// Test case 2: Valid share
	validShare := &RDAShare{
		Row:      5,
		Col:      2,   // Column 2
		SymbolID: 258, // 1 * 256 + 2 = column 2 ✓
		Data:     []byte("valid"),
		Height:   100,
	}

	err = bridge.StoreShare(ctx, validShare)
	require.NoError(t, err, "Valid share should be stored")

	// Kiểm chứng both RDA and blockstore have it
	rdaData, err := bridge.GetShare(ctx, 100, 5, 2, 258)
	require.NoError(t, err)
	assert.Equal(t, validShare.Data, rdaData)

	block := bridge.createIPLDBlock(validShare)
	blockData, err := bs.Get(ctx, block.Cid())
	require.NoError(t, err)
	assert.Equal(t, validShare.Data, blockData.RawData())
}

// TestRDAStorageWithBlockstore_BridgeForward_Compatibility
// kiểm chứng bridge là transparent wrapper (tất cả RDA methods work through bridge)
func TestRDAStorageWithBlockstore_BridgeForward_Compatibility(t *testing.T) {
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)

	rdaConfig := RDAStorageConfig{
		MyRow:    3,
		MyCol:    7,
		GridSize: 256,
	}
	rdaStorage := NewRDAStorage(rdaConfig)
	bridge := NewRDAStorageWithBlockstore(rdaStorage, bs)

	// Test VerifySymbolToColumnMapping
	err := bridge.VerifySymbolToColumnMapping(7) // 7 % 256 = 7 ✓
	assert.NoError(t, err)

	err = bridge.VerifySymbolToColumnMapping(8) // 8 % 256 = 8 ✗
	assert.Error(t, err)

	// Test VerifyGridPosition
	err = bridge.VerifyGridPosition(3, 7)
	assert.NoError(t, err)

	err = bridge.VerifyGridPosition(256, 0) // Out of grid
	assert.Error(t, err)

	// Test ComputeSymbolID
	symbolID, err := bridge.ComputeSymbolID(3, 7)
	require.NoError(t, err)
	expected := uint32(3*256 + 7)
	assert.Equal(t, expected, symbolID)
}

// TestRDAStorageWithBlockstore_CIDConsistency
// kiểm chứng rằng cùng share data luôn tạo cùng CID
// (important cho content-addressable retrieval)
func TestRDAStorageWithBlockstore_CIDConsistency(t *testing.T) {
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)

	rdaConfig := RDAStorageConfig{
		MyRow:    0,
		MyCol:    0,
		GridSize: 256,
	}
	rdaStorage := NewRDAStorage(rdaConfig)
	bridge := NewRDAStorageWithBlockstore(rdaStorage, bs)

	shareData := []byte("consistent data")
	share1 := &RDAShare{
		Row:      0,
		Col:      0,
		SymbolID: 0,
		Data:     shareData,
		Height:   100,
	}
	share2 := &RDAShare{
		Row:      1,
		Col:      0,
		SymbolID: 256,
		Data:     shareData, // Cùng data
		Height:   101,
	}

	block1 := bridge.createIPLDBlock(share1)
	block2 := bridge.createIPLDBlock(share2)

	// Cùng data ⟹ cùng CID
	assert.Equal(t, block1.Cid(), block2.Cid(), "Same data should produce same CID")

	// Khác data ⟹ khác CID
	share3Data := []byte("different data")
	share3 := &RDAShare{
		Row:      2,
		Col:      0,
		SymbolID: 512,
		Data:     share3Data,
		Height:   102,
	}
	block3 := bridge.createIPLDBlock(share3)
	assert.NotEqual(t, block1.Cid(), block3.Cid(), "Different data should produce different CID")
}
