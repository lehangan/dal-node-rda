package share

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockRDANodeService is a mock RDA service for testing.
type MockRDANodeService struct {
	Started bool
	Stopped bool
}

func (m *MockRDANodeService) Start(ctx context.Context) error {
	m.Started = true
	return nil
}

func (m *MockRDANodeService) Stop(ctx context.Context) error {
	m.Stopped = true
	return nil
}

func TestRDALifecycle_WaitForSubnetDiscovery(t *testing.T) {
	mockService := &MockRDANodeService{}
	lifecycle := NewRDALifecycle(mockService)

	// Mark as ready immediately
	go func() {
		time.Sleep(100 * time.Millisecond)
		lifecycle.MarkSubnetDiscoveryReady(nil)
	}()

	err := lifecycle.WaitForSubnetDiscovery(context.Background(), 5*time.Second)
	assert.NoError(t, err)
	assert.True(t, lifecycle.IsSubnetReady())
}

func TestRDALifecycle_WaitForSubnetDiscovery_Timeout(t *testing.T) {
	mockService := &MockRDANodeService{}
	lifecycle := NewRDALifecycle(mockService)

	err := lifecycle.WaitForSubnetDiscovery(context.Background(), 100*time.Millisecond)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
}

func TestRDALifecycle_WaitForSubnetDiscovery_WithError(t *testing.T) {
	mockService := &MockRDANodeService{}
	lifecycle := NewRDALifecycle(mockService)

	testErr := fmt.Errorf("subnet discovery failed")
	go func() {
		time.Sleep(100 * time.Millisecond)
		lifecycle.MarkSubnetDiscoveryReady(testErr)
	}()

	err := lifecycle.WaitForSubnetDiscovery(context.Background(), 5*time.Second)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "subnet discovery failed")
}

func TestRDALifecycle_IsSubnetReady(t *testing.T) {
	mockService := &MockRDANodeService{}
	lifecycle := NewRDALifecycle(mockService)

	assert.False(t, lifecycle.IsSubnetReady())

	lifecycle.MarkSubnetDiscoveryReady(nil)
	assert.True(t, lifecycle.IsSubnetReady())
}

func TestRDALifecycle_GetSubnetReadyChannel(t *testing.T) {
	mockService := &MockRDANodeService{}
	lifecycle := NewRDALifecycle(mockService)

	readyCh := lifecycle.GetSubnetReadyChannel()

	go func() {
		time.Sleep(100 * time.Millisecond)
		lifecycle.MarkSubnetDiscoveryReady(nil)
	}()

	select {
	case <-readyCh:
		// Expected
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for subnet ready")
	}
}

func TestRDALifecycle_Stop(t *testing.T) {
	mockService := &MockRDANodeService{}
	lifecycle := NewRDALifecycle(mockService)

	// Stop should not panic
	lifecycle.Stop()

	// Further operations should complete quickly
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := lifecycle.WaitForSubnetDiscovery(ctx, 100*time.Millisecond)
	require.Error(t, err)
}
