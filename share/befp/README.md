# RDA Fraud Proof (BEFP) Integration - Implementation Summary

**Status:** Framework Complete - Ready for Team Integration  
**Date:** March 16, 2026

## Overview

Triển khai Fraud Proof (specifically Bad Encoding Fraud Proof - BEFP) broadcast và subscription qua RDA Subnet, đảm bảo tất cả nodes trong grid nhận được BEFP khi có bad encoding được phát hiện.

## Architecture

### Problem Statement

Trong Celestia gốc, BEFP được broadcast qua unified pubsub topic. Tuy nhiên trong RDA:
- Nodes được tổ chức thành grid (row × column)
- Phải đảm bảo tất cả nodes trong grid nhận BEFP
- Broadcast qua cả ROW và COLUMN axes để lan truyền toàn grid

### Solution Design

```
detectBadEncoding() → BadEncodingProof created
    ↓
RDABEFPBroadcaster.Broadcast(befp)
    ├→ Publish to ALL row peers (PublishToRow)
    ├→ Publish to ALL column peers (PublishToCol)
    └→ Also fallback to standard fraud pubsub

RDABEFPSubscriber.StartListening()
    ├→ Listen from row subscription
    ├→ Listen from column subscription  
    └→ Re-propagate to other axes (flooding)
    
Result: Tất cả nodes nhận BEFP → Stop services
```

## Implementation Files

### 1. Broadcaster (`share/befp/broadcaster.go`)

**Key Components:**
- `Broadcaster` struct: Wraps RDASubnetManager + standard fraud broadcaster
- `Broadcast()` method: Publishes BEFP to row + column peers
- `GetMetrics()`: Tracks befp_received, befp_propagated, unique_proofs

**Workflow:**
```go
broadcaster := NewBroadcaster(config, subnetManager, standardBroadcaster)
err := broadcaster.Broadcast(ctx, befpProof)
// Automatically sends to:
// - All peers in this node's row
// - All peers in this node's column
// - Standard fraud pubsub (fallback)
```

### 2. Subscriber (`share/befp/subscriber.go`)

**Key Components:**
- `Subscriber` struct: Listens to RDA row + column subscriptions
- `StartListening()`: Begins listening to RDA subnet messages
- `handleBEFPMessage()`: Processes received BEFP
- `processBEFP()`: Marks valid proofs (triggers downstream services)

**Workflow:**
```go
subscriber := NewSubscriber(subnetManager, fraudService)
subscriber.StartListening(ctx)
// Automatically:
// - Listens from row and column channels
// - Deserializes received BEFP
// - Validates and processes
// - Allows fraud service to stop dependent services
```

### 3. Tests (`share/befp/broadcaster_test.go`, `integration_test.go`)

**Test Cases:**
- `TestBroadcaster_BroadcastBEFP`: Verifies BEFP sent to row + col
- `TestBroadcaster_DuplicateBEFP`: De-duplication logic
- `TestBroadcaster_Metrics`: Metrics tracking
- `TestSubscriber_ReceivesBEFP`: Subscription + processing
- Integration tests: End-to-end reconstruction + BEFP scenario

## Reconstruction + BEFP Flow

### Complete Workflow

```
Block arrives
    ↓
RDA STORE protocol processes shares
    ↓
Store in RDA column storage + blockstore (via bridge)
    ↓
Light node samples availability
    ↓
Full node attempts to reconstruct via Repair()
    ↓
[HAPPY PATH]: Repair succeeds ✓ → Continue
[FAILURE PATH]: Bad encoding detected ↓
    ↓
Create BEFP
    ↓
Broadcast via RDA Broadcaster
    ├→ Row peers alerted ↓
    └→ Column peers alerted ↓
    ↓
All peers receive via subscription
    ↓
Validate BEFP
    ↓
Mark block as Byzantine
    ↓
Stop DAS, Syncer, SubmitTx services
    ↓
Network consensus rejects this block
```

### Integration Points

**1. In Reconstruction (share/eds/retriever.go or similar):**
```go
eds, err := retriever.Retrieve(ctx, roots)
if err != nil {
    // Detect if ErrByzantine
    if isByzantine(err) {
        befp := CreateBadEncodingProof(...)
        // Broadcast via RDA network
        broadcaster.Broadcast(ctx, befp)
    }
}
```

**2. In Node Initialization (nodebuilder/):**
```go
broadcaster := befp.NewBroadcaster(
    rdaConfig,
    rdaSubnetManager,
    fraudBroadcaster,
)

subscriber := befp.NewSubscriber(
    rdaSubnetManager,
    fraudService,
)

subscriber.StartListening(ctx)
```

**3. In RDA STORE Handler:**
```go
// BEFP messages from network are received via
// RDASubnetManager.ReceiveFromRow() / ReceiveFromCol()
// and processed by Subscriber
```

## Key Features

### ✅ Implemented

1. **Dual-Axis Broadcasting**
   - Publishes BEFP to both row and column peers
   - Ensures network-wide propagation
   - Complements standard Celestia fraud broadcast

2. **De-duplication**
   - Tracks received proofs by (height, axis, index)
   - Prevents re-broadcasting same BEFP multiple times
   - Per-peer deduplication

3. **Metrics & Monitoring**
   - `befp_received`: Number of proofs processed
   - `befp_propagated`: Peer broadcast count
   - `unique_proofs`: Unique proofs seen

4. **Fallback Path**
   - Blockstore errors don't break RDA
   - Standard fraud broadcaster as fallback
   - Graceful degradation

### 🔧 Configuration

```go
config := share.RDAStorageConfig{
    MyRow:    0,        // This node's row
    MyCol:    0,        // This node's column
    GridSize: 256,      // K (grid dimension)
}

broadcaster := befp.NewBroadcaster(
    config,
    subnetManager,      // Manages RDA pubsub topics
    fraudBroadcaster,   // Standard Celestia broadcaster
)
```

## Testing Strategy

### Unit Tests
- Broadcaster behavior (send, de-duplicate, metrics)
- Subscriber behavior (receive, process, metrics)
- Mock subnet manager for isolation

### Integration Tests
- End-to-end: Byzantine detection → BEFP → Network propagation
- Realistic scenarios with multiple nodes
- Propagation path verification across grid

### Run Tests
```bash
# Unit tests
go test -timeout 30s ./share/befp -run "Broadcaster|Subscriber" -v

# Integration tests
go test -timeout 60s ./share/befp -run "Integration|Scenario" -v

# All BEFP tests
go test -timeout 60s ./share/befp -v
```

## Deployment Checklist

- [ ] Implement BEFP creation in BadEncoding detection
- [ ] Wire Broadcaster into reconstruction path
- [ ] Wire Subscriber into node initialization
- [ ] Update RDA protocol to handle BEFP messages
- [ ] Test with multi-node RDA network
- [ ] Verify pubsub topic subscriptions work
- [ ] Add monitoring/alerting for BEFP events
- [ ] Integration test with production-like grid

## Known Limitations & Future Work

### Current Limitations
1. De-duplication is in-process only (doesn't survive restarts)
   - **Fix:** Persist proofs in datastore key `befp/<hash>`

2. Mock subnet manager in tests doesn't fully implement RDASubnetManager
   - **Fix:** Implement full interface with actual topic/peer management

3. BEFP validation is simplified (doesn't validate Merkle proofs)
   - **Fix:** Use `byzantine.BadEncodingProof.Validate(header)`

### Future Enhancements
1. **BEFP Aggregation**: Batch multiple BEFPs before broadcast
2. **Adaptive Broadcasting**: Adjust peer set based on network topology
3. **BEFP Caching**: LRU cache for recent proofs
4. **Performance Analysis**: Measure latency from creation to all-nodes-received

## Related Documentation

- [RDA_RECONSTRUCTION_FIX.md](../RDA_RECONSTRUCTION_FIX.md) - Reconstruction bridge implementation
- [RECONSTRUCTION_COMPATIBILITY_ANALYSIS.md](../RECONSTRUCTION_COMPATIBILITY_ANALYSIS.md) - Detailed analysis
- [ADR #006](../docs/adr/adr-006-fraud-service.md) - Celestia Fraud Service architecture

## References

- RDA Paper: Reconstruction via column/row axes
- Celestia BEFP: Merkle proof-based fraud detection
- Byzantine agreement: Honest nodes must learn about bad encoding

## Files Created/Modified

```
NEW:
  share/befp/broadcaster.go          ~520 lines
  share/befp/subscriber.go           ~200 lines
  share/befp/broadcaster_test.go     ~380 lines
  share/befp/integration_test.go     ~400 lines
  share/befp/doc.md                  (this file)

NO CHANGES TO:
  share/rda_storage.go               (no change - works with bridge)
  share/rda_blockstore_bridge.go     (already done in prev session)
  share/eds/byzantine/bad_encoding.go (no change - compatible)
```

---

## Summary

**Completed:**
✅ RDA BEFP Broadcaster (pubsub to row+column)
✅ RDA BEFP Subscriber (listen and process)
✅ Metrics & monitoring
✅ Test framework (unit + integration)
✅ Documentation

**Status:** Ready for integration  
**Next:** Wire into reconstruction path and node initialization
**Testing:** Run full befp test suite to verify
