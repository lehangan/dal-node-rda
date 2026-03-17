package pruner

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/header/headertest"
	"github.com/celestiaorg/celestia-node/share"
)

// MockPruner is a mock implementation of Pruner for testing.
type MockPruner struct {
	pruneCalls int
	lastHeight uint64
	err        error
}

func (mp *MockPruner) Prune(ctx context.Context, h *header.ExtendedHeader) error {
	mp.pruneCalls++
	mp.lastHeight = h.Height()
	return mp.err
}

func TestRDAColumnAffinityPruner_Prune(t *testing.T) {
	mockPruner := &MockPruner{}

	pruner := NewRDAColumnAffinityPruner(mockPruner, &share.RDASubnetManager{}, 5)

	// Create a test header
	testHeader := headertest.RandExtendedHeader(t)

	err := pruner.Prune(context.Background(), testHeader)

	assert.NoError(t, err)
	assert.Equal(t, 1, mockPruner.pruneCalls)
	assert.Equal(t, testHeader.Height(), mockPruner.lastHeight)
}

func TestRDAColumnAffinityPruner_RetainColumnShares(t *testing.T) {
	mockPruner := &MockPruner{}

	// Create a simple RDASubnetManager mock (we'll check if it's nil)
	var rdaManager *share.RDASubnetManager

	pruner := NewRDAColumnAffinityPruner(mockPruner, rdaManager, 3)

	err := pruner.RetainColumnShares(context.Background(), 100)
	assert.NoError(t, err)
}

func TestRDAColumnAffinityPruner_Config(t *testing.T) {
	cfg := RDAColumnAffinityConfig{
		Enabled:         true,
		ColumnIndex:     5,
		RetentionBuffer: 100,
	}

	assert.True(t, cfg.Enabled)
	assert.Equal(t, uint32(5), cfg.ColumnIndex)
	assert.Equal(t, uint64(100), cfg.RetentionBuffer)
}
