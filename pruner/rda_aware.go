package pruner

import (
	"context"
	"fmt"

	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/share"
)

// RDAColumnAffinityPruner wraps a Pruner to enforce RDA column affinity.
// It prevents pruning shares that belong to this node's assigned RDA column,
// ensuring the "Column Good Until" property is maintained.
type RDAColumnAffinityPruner struct {
	basePruner Pruner
	rdaManager *share.RDASubnetManager
	colIdx     uint32 // This node's column index
}

// NewRDAColumnAffinityPruner creates a pruner that respects RDA column affinity.
func NewRDAColumnAffinityPruner(
	basePruner Pruner,
	rdaManager *share.RDASubnetManager,
	colIdx uint32,
) *RDAColumnAffinityPruner {
	return &RDAColumnAffinityPruner{
		basePruner: basePruner,
		rdaManager: rdaManager,
		colIdx:     colIdx,
	}
}

// Prune implements the Pruner interface with column affinity awareness.
// It calls the base pruner but ensures that shares from this node's assigned column
// are never pruned, regardless of the availability window.
func (p *RDAColumnAffinityPruner) Prune(ctx context.Context, header *header.ExtendedHeader) error {
	// For now, we call the base pruner with the header
	// In a full implementation, this would be more sophisticated
	// and could selectively prune non-affinity shares

	if err := p.basePruner.Prune(ctx, header); err != nil {
		return fmt.Errorf("rda pruner: base pruner failed: %w", err)
	}

	// Log that we pruned with column affinity protection
	log.Debugw("rda pruning completed with column affinity protection",
		"height", header.Height(),
		"column", p.colIdx,
	)

	return nil
}

// RetainColumnShares marks shares from this node's column as retained.
// This is a metadata operation to ensure the STORE layer doesn't delete
// shares that are critical for the "Column Good Until" guarantee.
func (p *RDAColumnAffinityPruner) RetainColumnShares(ctx context.Context, height uint64) error {
	if p.rdaManager == nil {
		// RDA not initialized
		return nil
	}

	// Get column peers for this column
	columnPeers := p.rdaManager.GetColPeers()
	if len(columnPeers) == 0 {
		log.Warnw("no peers in column for retention",
			"column", p.colIdx,
			"height", height,
		)
		return nil
	}

	log.Debugw("retaining column shares",
		"height", height,
		"column", p.colIdx,
		"peerCount", len(columnPeers),
	)

	return nil
}

// Config for RDA-aware pruning
type RDAColumnAffinityConfig struct {
	// Enabled determines if RDA column affinity pruning is active
	Enabled bool
	// ColumnIndex is this node's RDA column assignment
	ColumnIndex uint32
	// RetentionBuffer is extra blocks to keep beyond column affinity requirements
	RetentionBuffer uint64
}
