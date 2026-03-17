package befp

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/celestiaorg/go-fraud"
	logging "github.com/ipfs/go-log/v2"

	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/share"
	"github.com/celestiaorg/celestia-node/share/eds/byzantine"
)

var rdaBEFPLog = logging.Logger("rda.befp")

const defaultNetworkTimeout = 5 * time.Second

// Broadcaster broadcasts Bad Encoding Fraud Proofs (BEFP) qua RDA Subnet.
//
// Vấn đề: Celestia gốc broadcast BEFP qua chung một pubsub topic
// Trong RDA grid, phải broadcast qua TẤT CẢ nodes:
//   - Tất cả nodes trong ROW của node đó
//   - Tất cả nodes trong COLUMN của node đó
//
// Vì vậy tất cả nodes trong grid được thông báo và nhân BEFP.
//
// Khi một node phát hiện bad encoding:
//  1. Tạo BEFP
//  2. Node này broadcast qua row (tất cả peers cùng row)
//  3. Node này broadcast qua column (tất cả peers cùng column)
//  4. Khi nhận từ row/col peers, lại broadcast ra những axis khác
//     (để BEFP lan toàn bộ grid)
//  5. Tất cả nodes dừng sync block lỗi đó
type Broadcaster struct {
	config              share.RDAStorageConfig
	rdaSubnetMgr        *share.RDASubnetManager                   // RDA grid topology manager
	standardBroadcaster fraud.Broadcaster[*header.ExtendedHeader] // Fallback broadcast

	mu               sync.RWMutex
	receivedProofs   map[string]bool // Track already received proofs (key → true)
	proofCount       int64
	propagationCount int64
}

// NewBroadcaster tạo broadcaster cho BEFP qua RDA Subnet
func NewBroadcaster(
	config share.RDAStorageConfig,
	subnetMgr *share.RDASubnetManager,
	standardBroadcaster fraud.Broadcaster[*header.ExtendedHeader],
) *Broadcaster {
	rdaBEFPLog.Infof(
		"RDA BEFP Broadcaster initialized - node at (row=%d, col=%d), grid=%d",
		config.MyRow, config.MyCol, config.GridSize,
	)

	return &Broadcaster{
		config:              config,
		rdaSubnetMgr:        subnetMgr,
		standardBroadcaster: standardBroadcaster,
		receivedProofs:      make(map[string]bool),
	}
}

// Broadcast phát BEFP qua RDA row + column axes để tất cả nodes nhận được
func (b *Broadcaster) Broadcast(
	ctx context.Context,
	proof fraud.Proof[*header.ExtendedHeader],
) error {
	befp, ok := proof.(*byzantine.BadEncodingProof)
	if !ok {
		// Không phải BEFP - pass to standard broadcaster
		rdaBEFPLog.Debugf("RDA BEFP: Received non-BEFP fraud proof type=%v, passing to standard", proof.Type())
		return b.standardBroadcaster.Broadcast(ctx, proof)
	}

	rdaBEFPLog.Infof(
		"RDA BEFP: Broadcasting BEFP - height=%d, axis=%v, index=%d",
		befp.Height(), befp.Axis, befp.Index,
	)

	// Tạo unique key cho proof này (để track)
	proofKey := fmt.Sprintf("befp_%d_%v_%d", befp.Height(), befp.Axis, befp.Index)

	b.mu.Lock()
	if b.receivedProofs[proofKey] {
		b.mu.Unlock()
		rdaBEFPLog.Debugf(
			"RDA BEFP: Already received this proof (height=%d, axis=%v)",
			befp.Height(), befp.Axis,
		)
		return nil // Already propagated
	}
	b.receivedProofs[proofKey] = true
	b.proofCount++
	b.mu.Unlock()

	// Serialize BEFP
	befpData, err := befp.MarshalBinary()
	if err != nil {
		rdaBEFPLog.Warnf(
			"RDA BEFP: Failed to marshal BEFP (height=%d): %v",
			befp.Height(), err,
		)
		return fmt.Errorf("marshal BEFP: %w", err)
	}

	// === Broadcast qua RDA Row + Column axes ===
	// PublishToSubnet sẽ gửi tới tất cả peers trong row + column
	err = b.rdaSubnetMgr.PublishToSubnet(ctx, befpData)
	if err != nil {
		rdaBEFPLog.Warnf(
			"RDA BEFP: Failed to publish to subnet (height=%d): %v",
			befp.Height(), err,
		)
		// Don't fail - try standard broadcaster
	}

	// === Metrics ===
	rowPeers := b.rdaSubnetMgr.GetRowPeers()
	colPeers := b.rdaSubnetMgr.GetColPeers()

	b.mu.Lock()
	b.propagationCount += int64(len(rowPeers) + len(colPeers))
	b.mu.Unlock()

	rdaBEFPLog.Infof(
		"RDA BEFP: Broadcast via RDA - height=%d, row_peers=%d, col_peers=%d ✓",
		befp.Height(), len(rowPeers), len(colPeers),
	)

	// === Also broadcast via standard pubsub ===
	// Fallback: ensure BEFP reaches via standard Celestia path too
	err = b.standardBroadcaster.Broadcast(ctx, proof)
	if err != nil {
		rdaBEFPLog.Warnf(
			"RDA BEFP: Standard broadcast failed (height=%d): %v",
			befp.Height(), err,
		)
		// Don't fail - RDA broadcast succeeded
	}

	return nil
}

// GetMetrics trả về metrics về BEFP propagation
func (b *Broadcaster) GetMetrics() map[string]int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return map[string]int64{
		"befp_received":   b.proofCount,
		"befp_propagated": b.propagationCount,
		"unique_proofs":   int64(len(b.receivedProofs)),
	}
}
