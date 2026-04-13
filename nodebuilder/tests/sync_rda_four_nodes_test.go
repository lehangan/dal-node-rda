//go:build sync || integration

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/celestiaorg/celestia-node/api/rpc/client"
	dasmod "github.com/celestiaorg/celestia-node/das"
	"github.com/celestiaorg/celestia-node/nodebuilder/node"
	"github.com/celestiaorg/celestia-node/nodebuilder/tests/swamp"
)

// TestRDAFourLightNodesCatchUp validates that four DAS light nodes can run in RDA mode,
// form ready subnets, and catch up/successfully sample the chain.
func TestRDAFourLightNodesCatchUp(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping RDA four-node sync test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), swamp.DefaultTestTimeout)
	t.Cleanup(cancel)

	sw := swamp.NewSwamp(t, swamp.WithBlockTime(sbtime))

	// Fill a non-empty chain so DAS has data to sample.
	_, fillDone := swamp.FillBlocks(ctx, sw.ClientContext, sw.Accounts[0], bsize, numBlocks)
	sw.WaitTillHeight(ctx, numBlocks)

	bridge := sw.NewBridgeNode()
	require.NoError(t, bridge.Start(ctx))
	sw.SetBootstrapper(t, bridge)

	bridgeClient := getAdminClient(ctx, bridge, t)
	head, err := bridgeClient.Header.WaitForHeight(ctx, numBlocks)
	require.NoError(t, err)

	const lightNodes = 4
	lightClients := make([]*client.Client, 0, lightNodes)

	for i := 0; i < lightNodes; i++ {
		cfg := sw.DefaultTestConfig(node.Light)
		cfg.DASer.Mode = dasmod.ModeRDA
		cfg.DASer.RDAFallbackEnabled = false
		cfg.Share.RDAExpectedNodeCount = lightNodes

		ln := sw.NewNodeWithConfig(node.Light, cfg)
		require.NoError(t, ln.Start(ctx))
		cl := getAdminClient(ctx, ln, t)
		lightClients = append(lightClients, cl)
	}

	for i, cl := range lightClients {
		h, err := cl.Header.WaitForHeight(ctx, head.Height())
		require.NoErrorf(t, err, "light%d failed to reach target height", i+1)
		require.Equalf(t, head.Height(), h.Height(), "light%d reached unexpected height", i+1)
	}

	for i, cl := range lightClients {
		require.Eventuallyf(t, func() bool {
			diag, err := cl.DAS.RDADiagnostics(ctx)
			if err != nil {
				return false
			}
			return diag.SubnetReady && diag.Topology.TotalSubnetPeers > 0
		}, time.Minute, 2*time.Second, "light%d subnet discovery never became ready", i+1)
	}

	for i, cl := range lightClients {
		require.NoErrorf(t, cl.Share.SharesAvailable(ctx, head.Height()), "light%d failed SharesAvailable", i+1)
		require.NoErrorf(t, cl.DAS.WaitCatchUp(ctx), "light%d failed DAS catchup", i+1)

		mode, err := cl.DAS.RuntimeMode(ctx)
		require.NoErrorf(t, err, "light%d failed RuntimeMode", i+1)
		assert.Equalf(t, dasmod.ModeRDA, mode.Mode, "light%d is not running in RDA mode", i+1)
	}

	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case err := <-fillDone:
		require.NoError(t, err)
	}
}
