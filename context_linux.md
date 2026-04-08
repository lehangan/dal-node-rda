# RDA-aware DAS Linux Test Context (2026-04-08)

## 1. Environment

- OS: Linux
- Repository: celestiaorg/celestia-node
- Branch used during validation: implement-availability (and later context confirms active branch implement-das)

## 2. Goal of this run

- Re-validate current RDA-aware DAS status on Linux.
- Close/verify items listed in [context.md](context.md), especially integration confidence gaps and build/unit health.

## 3. Commands executed and outcomes

### 3.1 API auth integration gap from Windows

- Command: go test ./api -run TestAuthedRPC -count=1
- Result: PASS

### 3.2 nodebuilder/tests discovery by tag

- Command: go test ./nodebuilder/tests -tags share -list .
  - Result: only TestShareModule exists under share tag.
- Command: go test ./nodebuilder/tests -tags p2p -list .
  - Result: found RDA/discovery tests:
    - TestRDA_GridAutoSize
    - TestRDA_GridDistribution_MockNet
    - TestRDA_BridgeNode_Service
    - TestRDA_Discovery_BootstrapAndRendezvous
    - TestRDA_Discovery_DHT_PeerFinding
    - plus baseline discovery tests
- Command: go test ./nodebuilder/tests -tags reconstruction -list .
  - Result: found reconstruction tests:
    - TestFullReconstructFromBridge
    - TestFullReconstructFromFulls
    - TestFullReconstructFromLights

### 3.3 Integration execution

- Command: go test ./nodebuilder/tests -tags p2p -run 'TestBridgeNodeAsBootstrapper|TestFullDiscoveryViaBootstrapper|TestRestartNodeDiscovery|TestRDA_GridAutoSize|TestRDA_GridDistribution_MockNet|TestRDA_BridgeNode_Service|TestRDA_Discovery_BootstrapAndRendezvous|TestRDA_Discovery_DHT_PeerFinding' -count=1
- Result: PASS

- Command: go test ./nodebuilder/tests -tags reconstruction -run 'TestFullReconstructFromBridge|TestFullReconstructFromFulls|TestFullReconstructFromLights' -count=1
- Result: PASS

- Command: go test ./nodebuilder/tests -tags share -run TestShareModule -count=1
- Result: FAIL (reproducible across reruns)

## 4. Important note about build tags

- [nodebuilder/tests/share_test.go](nodebuilder/tests/share_test.go#L1) has build constraint: share || integration.
- Running without tag (example: go test -run '^TestShareModule$' ./nodebuilder/tests) gives:
  - PASS [no tests to run]
- This is a false positive because the file is excluded unless -tags share (or -tags integration) is set.

## 5. Root-cause findings from TestShareModule debug

### 5.1 Startup timeout path

- Failure point in test: [nodebuilder/tests/share_test.go](nodebuilder/tests/share_test.go#L44) (lightNode.Start(ctx))
- Test-wide context is 25s: [nodebuilder/tests/share_test.go](nodebuilder/tests/share_test.go#L26)
- Light node startup timeout is 20s: [nodebuilder/node/config.go](nodebuilder/node/config.go#L17)
- Node start error message generated in [nodebuilder/node.go](nodebuilder/node.go#L113)
- share module RDA lifecycle waits for subnet readiness during startup:
  - [nodebuilder/share/module.go](nodebuilder/share/module.go#L201)
  - [nodebuilder/share/module.go](nodebuilder/share/module.go#L213)
- Conclusion: startup can exceed effective timeout budget and fail with context deadline exceeded.

### 5.2 Additional cleanup issue when bypassing wait

- Debug command with bootstrap bypass:
  - CELESTIA_BOOTSTRAPPER=true go test -tags share -run '^TestShareModule$' ./nodebuilder/tests -count=1
- Behavior:
  - Test cases run much further.
  - Then fail on stop timeout in swamp cleanup.
- Cleanup stop timeout in swamp is only 1s:
  - [nodebuilder/tests/swamp/swamp.go](nodebuilder/tests/swamp/swamp.go#L93)

## 6. Build and unit suite status

- Command: make build
- Result: PASS (explicit BUILD_OK confirmation)

- Command: make test-unit
- Result: FAIL
- Observed primary failure in core package due to port collision:
  - panic: failed to listen on 127.0.0.1:44550
  - bind: address already in use

## 7. Current confidence snapshot (Linux)

- API auth gap: cleared (PASS).
- RDA/discovery integration (p2p): PASS.
- Reconstruction integration: PASS.
- share-tag integration: currently blocked by startup/cleanup timeout behavior in TestShareModule.
- Full unit confidence: blocked by intermittent environment/runtime port collision in core tests.

## 8. Recommended immediate next steps

1. Stabilize [nodebuilder/tests/share_test.go](nodebuilder/tests/share_test.go) startup context strategy for light node under RDA lifecycle wait.
2. Stabilize swamp cleanup timeout in [nodebuilder/tests/swamp/swamp.go](nodebuilder/tests/swamp/swamp.go#L93) to avoid stop-time false failures.
3. Re-run share integration with proper tag:
   - go test ./nodebuilder/tests -tags share -run '^TestShareModule$' -count=1
4. Re-run full target suite:
   - make build
   - make test-unit
   - optional: go test ./... -count=1
