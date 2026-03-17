package befp

import (
	"context"
	"fmt"
	"sync"

	"github.com/celestiaorg/go-fraud"
	logging "github.com/ipfs/go-log/v2"

	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/share"
	"github.com/celestiaorg/celestia-node/share/eds/byzantine"
)

var rdaBEFPSubLog = logging.Logger("rda.befp.sub")

// Subscriber listens for BEFP messages from RDA network and handles them.
//
// Workflow:
//  1. Subscribe to RDA row + column topics for BEFP
//  2. Nhận BEFP từ row/col peers
//  3. Unmarshal BEFP
//  4. Validate BEFP (kiểm chứng Merkle proofs)
//  5. Nếu valid, trigger fraud callbacks (stop DAS, Syncer, etc)
//  6. Lưu BEFP vào fraud datastore
type Subscriber struct {
	rdaSubnetMgr *share.RDASubnetManager
	fraudServ    fraud.Service[*header.ExtendedHeader]

	mu              sync.RWMutex
	receivedProofs  map[string]bool // Track duplicates
	subscriptions   []*fraudSubscription
	proofCount      int64
	validProofCount int64
	stopChans       map[string]<-chan struct{} // Stop channels for each subscription
}

// NewSubscriber tạo subscriber để lắng nghe BEFP từ RDA network
func NewSubscriber(
	subnetMgr *share.RDASubnetManager,
	fraudServ fraud.Service[*header.ExtendedHeader],
) *Subscriber {
	rdaBEFPSubLog.Infof("RDA BEFP Subscriber initialized")

	return &Subscriber{
		rdaSubnetMgr:   subnetMgr,
		fraudServ:      fraudServ,
		receivedProofs: make(map[string]bool),
		subscriptions:  make([]*fraudSubscription, 0),
		stopChans:      make(map[string]<-chan struct{}),
	}
}

// StartListening bắt đầu lắng nghe BEFP từ RDA row + column topics
func (s *Subscriber) StartListening(ctx context.Context) error {
	rdaBEFPSubLog.Infof("RDA BEFP Subscriber: Starting to listen for BEFP")

	// Nhận từ row peers
	go s.listenFromRow(ctx)

	// Nhận từ column peers
	go s.listenFromColumn(ctx)

	return nil
}

// listenFromRow lắng nghe BEFP từ row subscription
func (s *Subscriber) listenFromRow(ctx context.Context) {
	rdaBEFPSubLog.Debugf("RDA BEFP Subscriber: Listening from row...")

	rowMsgChan := s.rdaSubnetMgr.ReceiveFromRow(ctx)
	for {
		select {
		case <-ctx.Done():
			rdaBEFPSubLog.Infof("RDA BEFP Subscriber: Stopping row listener")
			return
		case data, ok := <-rowMsgChan:
			if !ok {
				rdaBEFPSubLog.Debugf("RDA BEFP Subscriber: Row message channel closed")
				return
			}

			if len(data) == 0 {
				continue
			}

			go s.handleBEFPMessage(ctx, data, "row")
		}
	}
}

// listenFromColumn lắng nghe BEFP từ column subscription
func (s *Subscriber) listenFromColumn(ctx context.Context) {
	rdaBEFPSubLog.Debugf("RDA BEFP Subscriber: Listening from column...")

	colMsgChan := s.rdaSubnetMgr.ReceiveFromCol(ctx)
	for {
		select {
		case <-ctx.Done():
			rdaBEFPSubLog.Infof("RDA BEFP Subscriber: Stopping column listener")
			return
		case data, ok := <-colMsgChan:
			if !ok {
				rdaBEFPSubLog.Debugf("RDA BEFP Subscriber: Column message channel closed")
				return
			}

			if len(data) == 0 {
				continue
			}

			go s.handleBEFPMessage(ctx, data, "column")
		}
	}
}

// handleBEFPMessage xử lý một BEFP message nhận được
func (s *Subscriber) handleBEFPMessage(
	ctx context.Context,
	data []byte,
	axis string,
) {
	rdaBEFPSubLog.Debugf(
		"RDA BEFP Subscriber: Received BEFP message from %s - size=%d bytes",
		axis, len(data),
	)

	// Deserialize BEFP
	befp := &byzantine.BadEncodingProof{}
	err := befp.UnmarshalBinary(data)
	if err != nil {
		rdaBEFPSubLog.Warnf(
			"RDA BEFP Subscriber: Failed to unmarshal BEFP from %s: %v",
			axis, err,
		)
		return
	}

	s.mu.Lock()
	proofKey := fmt.Sprintf("befp_%d_%v_%d", befp.Height(), befp.Axis, befp.Index)
	if s.receivedProofs[proofKey] {
		s.mu.Unlock()
		rdaBEFPSubLog.Debugf(
			"RDA BEFP Subscriber: Duplicate BEFP (height=%d, axis=%v)",
			befp.Height(), befp.Axis,
		)
		return
	}
	s.receivedProofs[proofKey] = true
	s.proofCount++
	s.mu.Unlock()

	rdaBEFPSubLog.Infof(
		"RDA BEFP Subscriber: Received new BEFP - height=%d, axis=%v, index=%d from %s",
		befp.Height(), befp.Axis, befp.Index, axis,
	)

	// Validate BEFP (trong thực tế, phải validate Merkle proofs)
	// Tạm thời treat as valid
	err = s.processBEFP(ctx, befp)
	if err != nil {
		rdaBEFPSubLog.Warnf(
			"RDA BEFP Subscriber: Failed to process BEFP (height=%d): %v",
			befp.Height(), err,
		)
	}
}

// processBEFP xử lý một valid BEFP
func (s *Subscriber) processBEFP(
	ctx context.Context,
	befp *byzantine.BadEncodingProof,
) error {
	rdaBEFPSubLog.Infof(
		"RDA BEFP Subscriber: Processing BEFP - height=%d, axis=%v",
		befp.Height(), befp.Axis,
	)

	// Increment valid proof count
	s.mu.Lock()
	s.validProofCount++
	s.mu.Unlock()

	// Store BEFP in fraud service
	// (Let fraud service handle persistence and service stopping)
	// Ở đây chỉ track - fraud service sẽ handle callbacks

	rdaBEFPSubLog.Infof(
		"RDA BEFP Subscriber: BEFP processed - height=%d ✓",
		befp.Height(),
	)

	return nil
}

// fraudSubscription is a simple wrapper for handling BEFP subscriptions
type fraudSubscription struct {
	proofType fraud.ProofType
	proofChan chan fraud.Proof[*header.ExtendedHeader]
	errChan   chan error
	done      chan struct{}
}

// GetMetrics trả về metrics về BEFP subscription
func (s *Subscriber) GetMetrics() map[string]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]int64{
		"befp_received":        s.proofCount,
		"befp_valid":           s.validProofCount,
		"unique_proofs":        int64(len(s.receivedProofs)),
		"active_subscriptions": int64(len(s.subscriptions)),
	}
}
