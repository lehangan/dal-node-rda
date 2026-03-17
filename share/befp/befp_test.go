package befp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/celestiaorg/celestia-node/share"
	"github.com/celestiaorg/celestia-node/share/eds/byzantine"
)

// TestCreateBroadcaster verifies broadcaster initialization
func TestCreateBroadcaster(t *testing.T) {
	config := share.RDAStorageConfig{
		MyRow:    1,
		MyCol:    2,
		GridSize: 256,
	}

	broadcaster := &Broadcaster{
		config:         config,
		receivedProofs: make(map[string]bool),
	}

	assert.NotNil(t, broadcaster)
	assert.Equal(t, uint32(1), broadcaster.config.MyRow)
	assert.Equal(t, uint32(2), broadcaster.config.MyCol)
}

// TestCreateSubscriber verifies subscriber initialization
func TestCreateSubscriber(t *testing.T) {
	subscriber := &Subscriber{
		receivedProofs: make(map[string]bool),
	}

	assert.NotNil(t, subscriber)
	metrics := subscriber.GetMetrics()
	assert.NotNil(t, metrics)
}

// TestBroadcasterMetrics verifies metrics structure
func TestBroadcasterMetrics(t *testing.T) {
	config := share.RDAStorageConfig{
		MyRow:    0,
		MyCol:    0,
		GridSize: 256,
	}

	broadcaster := &Broadcaster{
		config:           config,
		receivedProofs:   make(map[string]bool),
		proofCount:       42,
		propagationCount: 100,
	}

	metrics := broadcaster.GetMetrics()
	assert.Equal(t, int64(42), metrics["befp_received"])
	assert.Equal(t, int64(100), metrics["befp_propagated"])
	assert.Greater(t, len(metrics), 1)
}

// TestSubscriberMetrics verifies subscriber metrics structure
func TestSubscriberMetrics(t *testing.T) {
	subscriber := &Subscriber{
		receivedProofs:  make(map[string]bool),
		proofCount:      10,
		validProofCount: 8,
	}

	metrics := subscriber.GetMetrics()
	assert.Equal(t, int64(10), metrics["befp_received"])
	assert.Equal(t, int64(8), metrics["befp_valid"])
}

// TestBEFPProcessing_Basic tests basic BEFP creation and processing
func TestBEFPProcessing_Basic(t *testing.T) {
	// Create a simple BEFP (without using unexported fields)
	befp := &byzantine.BadEncodingProof{
		BlockHeight: 100,
		Shares:      make([]*byzantine.ShareWithProof, 0),
		Index:       2,
		Axis:        1,
	}

	// Verify BEFP can be marshaled
	data, err := befp.MarshalBinary()
	require.NoError(t, err, "Should marshal BEFP")
	assert.Greater(t, len(data), 0, "Marshaled data should not be empty")

	// Verify BEFP can be unmarshaled
	befp2 := &byzantine.BadEncodingProof{}
	err = befp2.UnmarshalBinary(data)
	require.NoError(t, err, "Should unmarshal BEFP")
	assert.Equal(t, befp.Height(), befp2.Height(), "Height should match")
}

// TestDeduplication_InMemory verifies deduplication logic
func TestDeduplication_InMemory(t *testing.T) {
	broadcaster := &Broadcaster{
		config:         share.RDAStorageConfig{},
		receivedProofs: make(map[string]bool),
	}

	// Simulate tracking received proofs
	key1 := "befp_100_1_2"
	key2 := "befp_100_1_2"
	key3 := "befp_100_2_2"

	broadcaster.receivedProofs[key1] = true
	broadcaster.proofCount = 1

	// Check deduplication
	assert.True(t, broadcaster.receivedProofs[key1])
	assert.True(t, broadcaster.receivedProofs[key2])  // Same key
	assert.False(t, broadcaster.receivedProofs[key3]) // Different proof

	assert.Equal(t, 1, len(broadcaster.receivedProofs))
	assert.Equal(t, int64(1), broadcaster.proofCount)
}

// TestBroadcasterConfig verifies configuration is set correctly
func TestBroadcasterConfig(t *testing.T) {
	config := share.RDAStorageConfig{
		MyRow:    5,
		MyCol:    10,
		GridSize: 256,
	}

	broadcaster := NewBroadcaster(config, nil, nil)
	assert.NotNil(t, broadcaster)
	assert.Equal(t, uint32(5), broadcaster.config.MyRow)
	assert.Equal(t, uint32(10), broadcaster.config.MyCol)
	assert.Equal(t, uint32(256), broadcaster.config.GridSize)
}

// TestSubscriberConfig verifies subscriber configuration
func TestSubscriberConfig(t *testing.T) {
	subscriber := NewSubscriber(nil, nil)
	assert.NotNil(t, subscriber)
	assert.Empty(t, subscriber.receivedProofs)
	assert.Equal(t, int64(0), subscriber.proofCount)
}
