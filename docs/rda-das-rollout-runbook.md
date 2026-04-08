# RDA-aware DAS Rollout and Rollback Runbook

## Scope

This runbook covers staged rollout, safety controls, and rollback for RDA-aware DAS.

RDA in this runbook means Robust Distributed Array.

## Preconditions

- Source gates are green:
  - make build
  - make test-unit
- Integration targets are green on Linux:
  - go test ./api -run TestAuthedRPC -count=1
  - go test ./nodebuilder/tests -tags share -run '^TestShareModule$' -count=1
  - go test ./nodebuilder/tests -tags p2p -run 'TestBridgeNodeAsBootstrapper|TestFullDiscoveryViaBootstrapper|TestRestartNodeDiscovery|TestRDA_GridAutoSize|TestRDA_GridDistribution_MockNet|TestRDA_BridgeNode_Service|TestRDA_Discovery_BootstrapAndRendezvous|TestRDA_Discovery_DHT_PeerFinding' -count=1
  - go test ./nodebuilder/tests -tags reconstruction -run 'TestFullReconstructFromBridge|TestFullReconstructFromFulls|TestFullReconstructFromLights' -count=1
- Container gates are green:
  - docker build -t celestia-node:rda-implement-das .
  - docker run --rm --entrypoint /bin/celestia celestia-node:rda-implement-das version

## Mode Controls (kill switch included)

The DAS mode is controlled by config and applied at process startup:

- classic: existing DAS path only
- rda: RDA path only
- hybrid: RDA first with classic fallback

Primary kill switch:

- Force classic mode by setting DAS mode to classic.

Secondary kill switch:

- Disable DAS module entirely with `enabled = false` in DAS config when emergency isolation is needed.

## Staged Rollout Plan

1. Staging canary

- Set mode to hybrid.
- Keep fallback enabled.
- Use conservative retry settings and query timeout defaults.
- Observe metrics for at least one full catchup window.

2. Staging scale-up

- Increase node count under hybrid mode.
- Validate no startup deadlocks and no repeated stop/start leaks.
- Confirm stable catchup behavior under moderate churn.

3. Testnet rollout

- Promote hybrid mode to testnet with same safety controls.
- Continue monitoring fallback ratio and retry depth trends.

4. Optional strict rollout

- Move to rda mode only after at least one stable period with healthy fallback and latency.

## SLO-Based Rollback Triggers

Trigger rollback to classic mode if one or more conditions persist beyond 15 minutes:

- Fallback ratio > 20% on healthy network conditions.
- Catchup lag increases continuously versus pre-rollout baseline.
- Retry depth p95 exceeds configured maximum retry budget for sustained periods.
- Repeated startup failures tied to subnet readiness lifecycle.

## Rollback Procedure

1. Set DAS mode to classic.
2. Ensure checkpoint backup is preserved before restart.
3. Restart nodes with updated config.
4. Verify catchup recovery and sampling health in classic mode.
5. Keep RDA diagnostics artifacts for postmortem analysis.

## Checkpoint Handling

- Do not delete checkpoints during rollback unless corruption is confirmed.
- Preserve a timestamped backup before any manual checkpoint intervention.

## Operational Verification After Mode Change

- Confirm node startup completes within expected startup timeout.
- Confirm headers progress and wait-catchup behavior remains healthy.
- Confirm API diagnostics endpoints respond and mode is reported correctly.

## Suggested Command Set

- Source validation:
  - make build
  - make test-unit
  - make test-unit-race
  - make test-integration TAGS=share
  - make test-integration TAGS=p2p
  - make test-integration TAGS=reconstruction
- Docker image validation:
  - docker build -t celestia-node:rda-implement-das .
  - docker run --rm --entrypoint /bin/celestia celestia-node:rda-implement-das version
  - docker run --rm -e NODE_TYPE=light -e P2P_NETWORK=mocha -e RDA_EXPECTED_NODES=16 celestia-node:rda-implement-das celestia light --help
- Optional multi-arch image build:
  - docker buildx build --platform linux/amd64,linux/arm64 -t <registry>/<repo>:rda-implement-das --push .

## Notes

- Docker image packaging is the primary deployment artifact for this rollout.
- Linux integration results remain the source of truth for release gating.
