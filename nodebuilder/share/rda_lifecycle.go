package share

import (
	"context"
	"fmt"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var rdalog = logging.Logger("nodebuilder/share/rda")

// RDALifecycle manages the startup/shutdown lifecycle of RDA-dependent services.
// It ensures that subnet discovery completes before allowing DAS/Store workers to start.
type RDALifecycle struct {
	rdaService interface{} // Generic service reference

	subnetReadyOnce sync.Once
	subnetReady     chan struct{}
	subnetErr       error

	ctx    context.Context
	cancel context.CancelFunc
}

// NewRDALifecycle creates a new RDA lifecycle manager.
func NewRDALifecycle(rdaService interface{}) *RDALifecycle {
	ctx, cancel := context.WithCancel(context.Background())
	return &RDALifecycle{
		rdaService:  rdaService,
		subnetReady: make(chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// WaitForSubnetDiscovery blocks until subnet discovery completes or timeout.
// This should be called before starting DAS and Store workers.
func (l *RDALifecycle) WaitForSubnetDiscovery(ctx context.Context, timeout time.Duration) error {
	rdalog.Info("waiting for RDA subnet discovery to complete")

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-l.subnetReady:
		if l.subnetErr != nil {
			return fmt.Errorf("rda subnet discovery failed: %w", l.subnetErr)
		}
		rdalog.Info("RDA subnet discovery completed successfully")
		return nil
	case <-ctx.Done():
		return fmt.Errorf("timeout waiting for RDA subnet discovery: %w", ctx.Err())
	case <-l.ctx.Done():
		return fmt.Errorf("RDA lifecycle cancelled")
	}
}

// MarkSubnetDiscoveryReady marks subnet discovery as complete.
// This is called by the RDA service after grid formation.
func (l *RDALifecycle) MarkSubnetDiscoveryReady(err error) {
	l.subnetReadyOnce.Do(func() {
		l.subnetErr = err
		close(l.subnetReady)
		if err != nil {
			rdalog.Errorw("subnet discovery failed", "err", err)
		} else {
			rdalog.Info("subnet discovery ready")
		}
	})
}

// Stop stops the RDA lifecycle manager.
func (l *RDALifecycle) Stop() {
	l.cancel()
}

// IsSubnetReady returns true if subnet discovery has completed.
func (l *RDALifecycle) IsSubnetReady() bool {
	select {
	case <-l.subnetReady:
		return true
	default:
		return false
	}
}

// GetSubnetReadyChannel returns the channel that is closed when subnet is ready.
// Useful for select statements waiting on subnet readiness.
func (l *RDALifecycle) GetSubnetReadyChannel() <-chan struct{} {
	return l.subnetReady
}
