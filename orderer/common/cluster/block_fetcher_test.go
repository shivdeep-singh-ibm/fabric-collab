/*
Copyright IBM Corp. 2022 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cluster_test

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/common/cluster/mocks"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/roundrobin"
)

func TestBlockFetcherHappyPath(t *testing.T) {
	bf := cluster.BlockFetcher{}
	bf.BlockSourceFactory = func(c cluster.FetcherConfig) cluster.BlockSource {
		return mock_block_puller(1, nil, time.Second*1)
	}

	// Inject Time Function
	bf.TimeNow = time.Now
	bf.ShuffleTimeoutThrehold = 50
	bf.ShuffleTimeout = time.Second * 1
	bf.Logger = flogging.MustGetLogger("test")

	// add 2 endpoints to pull from
	bf.Config.Endpoints = []cluster.EndpointCriteria{{Endpoint: "localhost:5100"}, {Endpoint: "localhost:5200"}}

	// pullblock from any endpoint
	require.Equal(t, uint64(1), bf.PullBlock(1).Header.Number)
}

func TestBlockFetcherShuffleTimeOut(t *testing.T) {
	// node1 and node2 send blocks after 12s. Shuffle timeout is set to 12s ,so
	// successive PullBlock calls to the block puller should fetch
	// blocks from the different nodes, since the source would be shuffled after 12s
	bf := cluster.BlockFetcher{}

	source_names := []string{"first", "second"}
	source_name_selector := 0
	getNameForSource := func() string {
		name := source_names[source_name_selector]
		source_name_selector = (source_name_selector + 1) % len(source_names)
		return name
	}

	// the block source created first sends blocks with data: "first"
	// the block source created seond time sends blocks with data: "second"
	// test uses this information to detetc whether blocksource has been shuffled while pulling blocks
	bf.BlockSourceFactory = func(c cluster.FetcherConfig) cluster.BlockSource {
		if c.Endpoints[0].Endpoint == "localhost:5100" {
			data := [][]byte{[]byte(getNameForSource())}
			return mock_block_puller(1, data, time.Second*12)
		} else {
			data := [][]byte{[]byte(getNameForSource())}
			return mock_block_puller(1, data, time.Second*12)
		}
	}

	// Inject Time Function
	bf.TimeNow = time.Now
	bf.ShuffleTimeoutThrehold = 50
	bf.ShuffleTimeout = time.Duration(12 * time.Second)
	bf.Logger = flogging.MustGetLogger("test")
	bf.Config.Endpoints = []cluster.EndpointCriteria{{Endpoint: "localhost:5100"}, {Endpoint: "localhost:5200"}}

	// First block will have block.Data.Data as : []byte{"first"}
	firstAttempt := string(bf.PullBlock(1).Data.Data[0])
	// Due to timeout, the source should have shuffled
	// Second block should be pulled from another node and have block.Data.data: []byte{"second"}
	secondAttempt := string(bf.PullBlock(1).Data.Data[0])
	require.Equal(t, "first", firstAttempt)
	require.Equal(t, "second", secondAttempt)
}

func TestBlockFetcherShuffleTimeOutDisable(t *testing.T) {
	// node1 and node2 send blocks after 12s. Shuffle timeout is disabled so
	// successive PullBlock calls to the block puller should fetch
	// blocks from the same node.
	bf := cluster.BlockFetcher{}

	source_names := []string{"first", "second"}
	source_name_selector := 0
	getNameForSource := func() string {
		name := source_names[source_name_selector]
		source_name_selector = (source_name_selector + 1) % len(source_names)
		return name
	}

	bf.BlockSourceFactory = func(c cluster.FetcherConfig) cluster.BlockSource {
		if c.Endpoints[0].Endpoint == "localhost:5100" {
			data := [][]byte{[]byte(getNameForSource())}
			return mock_block_puller(1, data, time.Second*12)
		} else {
			data := [][]byte{[]byte(getNameForSource())}
			return mock_block_puller(1, data, time.Second*12)
		}
	}
	// Inject Time Function
	bf.TimeNow = time.Now
	bf.ShuffleTimeoutThrehold = 50
	// Disable ShuffleTimeout
	bf.ShuffleTimeout = time.Duration(0)
	bf.Logger = flogging.MustGetLogger("test")
	bf.Config.Endpoints = []cluster.EndpointCriteria{{Endpoint: "localhost:5100"}, {Endpoint: "localhost:5200"}}

	// First block will have block.Data.Data as : []byte{"first"}
	firstAttempt := string(bf.PullBlock(1).Data.Data[0])
	// the source should have not have shuffled
	// Second block should be pulled from the same node and have block.Data.data: []byte{"first"}
	secondAttempt := string(bf.PullBlock(1).Data.Data[0])
	require.Equal(t, "first", firstAttempt)
	require.Equal(t, "first", secondAttempt)
}

func TestBlockFetcherNodeOffline(t *testing.T) {
	// node1 returns nil after 2s, so assuming it to be offline
	// source should be shuffled and the block should be pulled from node2
	bf := cluster.BlockFetcher{}

	source_names := []string{"node1", "node2"}
	source_name_selector := 0
	getNameForSource := func() string {
		name := source_names[source_name_selector]
		source_name_selector = (source_name_selector + 1) % len(source_names)
		return name
	}

	bf.BlockSourceFactory = func(c cluster.FetcherConfig) cluster.BlockSource {
		node_name := getNameForSource()
		if node_name == "node1" {
			return mock_block_puller_returns_nil(time.Second * 2)
		} else {
			return mock_block_puller(2, nil, time.Second*1)
		}
	}

	bf.TimeNow = time.Now
	bf.ShuffleTimeoutThrehold = 50
	// Disable ShuffleTimeout
	bf.ShuffleTimeout = time.Duration(0)
	bf.Logger = flogging.MustGetLogger("test")
	bf.Config.Endpoints = []cluster.EndpointCriteria{
		{Endpoint: "localhost:5100"},
		{Endpoint: "localhost:5200"},
	}
	bf.Config.FetchTimeout = time.Duration(time.Second * 10)

	require.Equal(t, uint64(2), bf.PullBlock(2).Header.Number)
}

func mock_attestation_puller_returns_nil(err error, after time.Duration) cluster.AttestationSource {
	bs := &mocks.AttestationSource{}

	bs.On("PullAttestation", mock.Anything).Return(func(s uint64) *orderer.BlockAttestation {
		return nil
	}, func(s uint64) error {
		return err
	}).After(after)

	bs.On("Close", mock.Anything)
	return bs
}

func mock_attestation_puller(seq uint64, after time.Duration) cluster.AttestationSource {
	bs := &mocks.AttestationSource{}
	attestation := &orderer.BlockAttestation{
		Header:   &common.BlockHeader{Number: seq},
		Metadata: &common.BlockMetadata{},
	}
	bs.On("PullAttestation", mock.Anything).Return(func(s uint64) *orderer.BlockAttestation {
		return attestation
	}, func(s uint64) error {
		return nil
	}).After(after)

	bs.On("Close", mock.Anything)
	return bs
}

func mock_block_puller_returns_nil(after time.Duration) cluster.BlockSource {
	bs := &mocks.BlockSource{}
	bs.On("PullBlock", mock.Anything).Return(nil).After(after)
	bs.On("UpdateEndpoints", mock.Anything)
	bs.On("Close", mock.Anything)
	return bs
}

func mock_block_puller(seq uint64, data [][]byte, after time.Duration) cluster.BlockSource {
	bs := &mocks.BlockSource{}
	bs.On("PullBlock", mock.Anything).Return(&common.Block{
		Header:   &common.BlockHeader{Number: seq},
		Data:     &common.BlockData{Data: data},
		Metadata: &common.BlockMetadata{},
	}).After(after)

	bs.On("UpdateEndpoints", mock.Anything)
	bs.On("Close", mock.Anything)
	return bs
}

func TestBlockFetcherBFTBehaviorBlockWithhold(t *testing.T) {
	// The first endpoint we try to pull Blocks witholds blocks,
	// the PullBlock should then suspect the endpoint and probe other endpoints to confirm the suspicion.
	// then it should shuffle the endpoint and pull blocks from another endpoint, which should succeed.
	bf := cluster.BlockFetcher{}

	source_names := []string{"node1", "node2", "node3", "node4", "node5", "node6", "node7", "node8", "node9", "node10"}
	source_name_selector := 0
	getNameForSource := func() string {
		name := source_names[source_name_selector]
		source_name_selector = (source_name_selector + 1) % len(source_names)
		return name
	}

	bf.BlockSourceFactory = func(c cluster.FetcherConfig) cluster.BlockSource {
		// node1 witholds block while other endpoints can deliver blocks
		node_name := getNameForSource()
		if node_name == "node1" {
			return mock_block_puller_returns_nil(time.Second * 4)
		}
		// the block puller below doesn't withold blocks
		data := [][]byte{[]byte(node_name)}
		// this block puller returns blocks with seq:1 and data after 2s
		return mock_block_puller(1, data, time.Second*2)
	}

	attestation_source_created := false
	bf.AttestationSourceFactory = func(c cluster.FetcherConfig) cluster.AttestationSource {
		if !attestation_source_created {
			// first attestation source created withholds attestation
			attestation_source_created = true
			return mock_attestation_puller_returns_nil(nil, time.Second*12)
		}
		// all other attestation pullers send atetstation blocks
		return mock_attestation_puller(1, time.Second*2)
	}

	// Add time
	bf.TimeNow = time.Now
	bf.ShuffleTimeoutThrehold = 50
	// Disable ShuffleTimeout
	bf.MaxByzantineNodes = 3
	bf.ShuffleTimeout = time.Duration(0)
	bf.Logger = flogging.MustGetLogger("test")
	bf.Config.FetchTimeout = time.Duration(time.Second * 3)

	bf.Config.Endpoints = []cluster.EndpointCriteria{
		{Endpoint: "localhost:5100"}, {Endpoint: "localhost:5101"}, {Endpoint: "localhost:5102"}, {Endpoint: "localhost:5103"}, {Endpoint: "localhost:5104"}, {Endpoint: "localhost:5105"}, {Endpoint: "localhost:5106"}, {Endpoint: "localhost:5107"}, {Endpoint: "localhost:5108"},
	}

	bf.ConfirmByzantineBehavior = func(b []*orderer.BlockAttestation) bool {
		// simulate byzantine behaviour
		return true
	}

	block := bf.PullBlock(1)
	block_data := string(block.Data.Data[0])
	require.NotEqual(t, "node1", block_data)
	require.Equal(t, uint64(1), block.Header.Number)
}

func TestBlockFetcherBFTBehaviorSuspicionNoBlockWithhold(t *testing.T) {
	// Endpoint node1 returns nil since the block may not be present
	// PullBlock will try to probe other endpoints to suspect the endpoint localhost:5100 for witholding
	// blocks but other endpoints will also return nil, So endpoint will not be shuffled and
	// PullBlock should return nil after retries are enhausted.
	bf := cluster.BlockFetcher{}

	source_names := []string{"node1", "node2", "node3", "node4", "node5", "node6", "node7", "node8", "node9", "node10"}
	source_name_selector := 0
	getNameForSource := func() string {
		name := source_names[source_name_selector]
		source_name_selector = (source_name_selector + 1) % len(source_names)
		return name
	}

	bf.BlockSourceFactory = func(c cluster.FetcherConfig) cluster.BlockSource {
		// node1 witholds block while other endpoints can deliver blocks.

		node_name := getNameForSource()
		if node_name == "node1" {
			return mock_block_puller_returns_nil(time.Second * 12)
		}
		// the block puller below doesn't withold blocks
		data := [][]byte{[]byte(node_name)}
		// this block puller returns blocks with seq:1 and data after 2s
		return mock_block_puller(1, data, time.Second*2)
	}

	bf.AttestationSourceFactory = func(c cluster.FetcherConfig) cluster.AttestationSource {
		// attestation source created withhold attestation
		return mock_attestation_puller_returns_nil(nil, time.Second*1)
	}

	// Add time
	bf.TimeNow = time.Now
	bf.ShuffleTimeoutThrehold = 50

	bf.ShuffleTimeoutThrehold = 50
	// Disable ShuffleTimeout
	bf.MaxPullBlockRetries = 2
	bf.MaxByzantineNodes = 3
	bf.ShuffleTimeout = time.Duration(0)
	bf.Logger = flogging.MustGetLogger("test")
	bf.Config.FetchTimeout = time.Duration(time.Second * 5)

	bf.Config.Endpoints = []cluster.EndpointCriteria{
		{Endpoint: "localhost:5100"},
		{Endpoint: "localhost:5101"},
		{Endpoint: "localhost:5102"},
		{Endpoint: "localhost:5103"},
		{Endpoint: "localhost:5101"},
		{Endpoint: "localhost:5102"},
		{Endpoint: "localhost:5103"},
		{Endpoint: "localhost:5102"},
		{Endpoint: "localhost:5103"},
	}

	// No byzantine behaviour should make sure that the
	// endpoint is not shuffled.
	bf.ConfirmByzantineBehavior = func(b []*orderer.BlockAttestation) bool {
		return false
	}

	// node1 withholds blocks, other nodes withold attestations,
	// node1 is not suspected for byzantine behaviour and
	block := bf.PullBlock(1)
	require.Equal(t, (*common.Block)(nil), block)
}

func TestBlockFetcherBFTBehaviorSuspicionListFull(t *testing.T) {
	// suspicion list of block fetcher (bf.suspects) can hold max MaxByzantineNodes entries, when it is full,
	// last entry is evicted to accomodate the new entry.
	// 2 malicious orderers
	// bf.MaxByzantineNodes = 1
	// Try to pull blocks from orderer, it witholds blocks,
	// add it to suspicion list which is now full and check next, it also witholds blocks
	// add it to list, which is full, so evict one element from the list
	// The first endpoint(node1) we try to pull Blocks witholds blocks,
	// the PullBlock should then suspect the endpoint and probe other endpoints to confirm the suspicion.
	// then this endpoint(node1) should be added to suspects list and blockfetcher should shuffle the endpoint and
	// pull blocks from another endpoint (node2), which should again withold blocks.
	// then it(node2) would be added to suspects list which is already full (MaxByzantineNodese=1),
	// hence one entry (node1) should be evicted from suspect lists and source shuffled to pull from next orderer (node1),
	// which is not byzantine now, since it is evicted from the suspect list, we should be able to pull from node1.
	// Test is added for coverage of eviction from suspect list
	bf := cluster.BlockFetcher{}

	source_names := []string{"node1", "node2", "node1", "node3"}
	source_name_selector := 0
	getNameForSource := func() string {
		name := source_names[source_name_selector]
		source_name_selector = (source_name_selector + 1) % len(source_names)
		return name
	}

	firstTime := true
	bf.BlockSourceFactory = func(c cluster.FetcherConfig) cluster.BlockSource {
		// node1 and node 2 withold block while other endpoints can deliver blocks
		node_name := getNameForSource()
		if (node_name == "node1" && firstTime) || node_name == "node2" {
			firstTime = false
			return mock_block_puller_returns_nil(time.Second * 4)
		}
		// the block puller below doesn't withold blocks
		data := [][]byte{[]byte(node_name)}
		// this block puller returns blocks with seq:1 and data after 2s
		return mock_block_puller(1, data, time.Second*2)
	}

	attestation_source_created := false
	bf.AttestationSourceFactory = func(c cluster.FetcherConfig) cluster.AttestationSource {
		if !attestation_source_created {
			// first attestation source created withholds attestation
			attestation_source_created = true
			return mock_attestation_puller_returns_nil(nil, time.Second*12)
		}
		// all other attestation pullers send atetstation blocks
		return mock_attestation_puller(1, time.Second*2)
	}

	// Add time
	bf.TimeNow = time.Now
	bf.ShuffleTimeoutThrehold = 50
	// Disable ShuffleTimeout
	bf.MaxByzantineNodes = 1
	bf.ShuffleTimeout = time.Duration(0)
	bf.Logger = flogging.MustGetLogger("test")
	bf.Config.FetchTimeout = time.Duration(time.Second * 3)

	bf.Config.Endpoints = []cluster.EndpointCriteria{
		{Endpoint: "localhost:5100"}, {Endpoint: "localhost:5101"}, {Endpoint: "localhost:5102"}, {Endpoint: "localhost:5103"}, {Endpoint: "localhost:5104"}, {Endpoint: "localhost:5105"}, {Endpoint: "localhost:5106"}, {Endpoint: "localhost:5107"}, {Endpoint: "localhost:5108"},
	}

	bf.ConfirmByzantineBehavior = func(b []*orderer.BlockAttestation) bool {
		// simulate byzantine behaviour
		return true
	}

	block := bf.PullBlock(1)
	block_data := string(block.Data.Data[0])
	require.NotEqual(t, "node2", block_data)
	require.Equal(t, "node1", block_data)
	require.Equal(t, uint64(1), block.Header.Number)
}

func TestBlockFetcherBFTBehaviorPullAttestationError(t *testing.T) {
	// test for coverage of error returned by : attestation, err := attestation_puller.PullAttestation(seq)
	// and attestation_puller.Close in case of error
	// The first endpoint we try to pull Blocks witholds blocks,
	// the PullBlock should then suspect the endpoint and probe other endpoints to confirm the suspicion.
	// then it should shuffle the endpoint and pull blocks from another endpoint, which should succeed.
	bf := cluster.BlockFetcher{}

	source_names := []string{"node1", "node2", "node3", "node4", "node5", "node6", "node7", "node8", "node9", "node10"}
	source_name_selector := 0
	getNameForSource := func() string {
		name := source_names[source_name_selector]
		source_name_selector = (source_name_selector + 1) % len(source_names)
		return name
	}

	bf.BlockSourceFactory = func(c cluster.FetcherConfig) cluster.BlockSource {
		// node1 witholds block while other endpoints can deliver blocks
		node_name := getNameForSource()
		if node_name == "node1" {
			return mock_block_puller_returns_nil(time.Second * 4)
		}
		// the block puller below doesn't withold blocks
		data := [][]byte{[]byte(node_name)}
		// this block puller returns blocks with seq:1 and data after 2s
		return mock_block_puller(1, data, time.Second*2)
	}

	attestation_source_created := false
	bf.AttestationSourceFactory = func(c cluster.FetcherConfig) cluster.AttestationSource {
		if !attestation_source_created {
			// first attestation source created returns error
			attestation_source_created = true
			return mock_attestation_puller_returns_nil(errors.New("connection failure"), time.Second*12)
		}
		// all other attestation pullers send atetstation blocks
		return mock_attestation_puller(1, time.Second*2)
	}

	// Add time
	bf.TimeNow = time.Now
	bf.ShuffleTimeoutThrehold = 50
	// Disable ShuffleTimeout
	bf.MaxByzantineNodes = 3
	bf.ShuffleTimeout = time.Duration(0)
	bf.Logger = flogging.MustGetLogger("test")

	var attestationError error
	closeAttestationPuller := false
	// set log level to debug for this test
	flogging.ActivateSpec("debug")
	defer flogging.ActivateSpec("info")
	// Add a hook to check for error and debug message
	bf.Logger = bf.Logger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, ("Error pulling attestation")) && strings.Contains(entry.Message, ("connection failure")) {
			attestationError = errors.New(entry.Message)
		}
		// comparing the debug log, so logging level should be debug for this test.
		if strings.Contains(entry.Message, ("Close previous attestation puller")) {
			closeAttestationPuller = true
		}

		return nil
	}))

	bf.Config.FetchTimeout = time.Duration(time.Second * 3)

	bf.Config.Endpoints = []cluster.EndpointCriteria{
		{Endpoint: "localhost:5100"}, {Endpoint: "localhost:5101"}, {Endpoint: "localhost:5102"}, {Endpoint: "localhost:5103"}, {Endpoint: "localhost:5104"}, {Endpoint: "localhost:5105"}, {Endpoint: "localhost:5106"}, {Endpoint: "localhost:5107"}, {Endpoint: "localhost:5108"},
	}

	bf.ConfirmByzantineBehavior = func(b []*orderer.BlockAttestation) bool {
		// simulate byzantine behaviour
		return true
	}

	block := bf.PullBlock(1)
	block_data := string(block.Data.Data[0])
	require.NotEqual(t, "node1", block_data)
	require.Equal(t, uint64(1), block.Header.Number)
	require.NotEqual(t, nil, attestationError)
	require.Equal(t, true, closeAttestationPuller)
}

func TestBlockFetcherBFTBehaviorAttestationsLessThanF(t *testing.T) {
	// The first endpoint we try to pull Blocks witholds blocks,
	// the PullBlock should then suspect the endpoint and probe other endpoints to confirm the suspicion.
	// While pulling attestations it is able to pull 1 attestation instead of 3. The blockfetcher should shuffle the endpoint and pull blocks from another endpoint, which should succeed.
	bf := cluster.BlockFetcher{}

	source_names := []string{"node1", "node2", "node3", "node4", "node5", "node6", "node7", "node8", "node9", "node10"}
	source_name_selector := 0
	getNameForSource := func() string {
		name := source_names[source_name_selector]
		source_name_selector = (source_name_selector + 1) % len(source_names)
		return name
	}

	bf.BlockSourceFactory = func(c cluster.FetcherConfig) cluster.BlockSource {
		// node1 witholds block while other endpoints can deliver blocks
		node_name := getNameForSource()
		if node_name == "node1" {
			return mock_block_puller_returns_nil(time.Second * 4)
		}
		// the block puller below doesn't withold blocks
		data := [][]byte{[]byte(node_name)}
		// this block puller returns blocks with seq:1 and data after 2s
		return mock_block_puller(1, data, time.Second*2)
	}

	attestation_source_created := false
	bf.AttestationSourceFactory = func(c cluster.FetcherConfig) cluster.AttestationSource {
		if !attestation_source_created {
			// first attestation source created returnsattestation block
			attestation_source_created = true
			return mock_attestation_puller(1, time.Second*1)
		}
		// all other attestation pullers send error
		return mock_attestation_puller_returns_nil(errors.New("connection failure"), time.Second*1)
	}

	// Add time
	bf.TimeNow = time.Now
	bf.ShuffleTimeoutThrehold = 50
	// Disable ShuffleTimeout
	bf.MaxByzantineNodes = 3
	bf.ShuffleTimeout = time.Duration(0)
	bf.Logger = flogging.MustGetLogger("test")

	var attestationError error
	// Add a hook to check for error and debug message
	bf.Logger = bf.Logger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, ("attestations pulled from")) {
			attestationError = errors.New("attestations pulled are less than F")
		}
		return nil
	}))

	bf.Config.FetchTimeout = time.Duration(time.Second * 3)

	bf.Config.Endpoints = []cluster.EndpointCriteria{
		{Endpoint: "localhost:5100"}, {Endpoint: "localhost:5101"}, {Endpoint: "localhost:5102"}, {Endpoint: "localhost:5103"}, {Endpoint: "localhost:5104"}, {Endpoint: "localhost:5105"}, {Endpoint: "localhost:5106"}, {Endpoint: "localhost:5107"}, {Endpoint: "localhost:5108"},
	}

	bf.ConfirmByzantineBehavior = func(b []*orderer.BlockAttestation) bool {
		// simulate byzantine behaviour
		return true
	}

	block := bf.PullBlock(1)
	block_data := string(block.Data.Data[0])
	require.NotEqual(t, "node1", block_data)
	require.Equal(t, uint64(1), block.Header.Number)
	require.NotEqual(t, nil, attestationError)
}

func TestBlockFetcherMaxRetriesExhausted(t *testing.T) {
	// Try to pull block from an orderer, it should withold blocks. while pulling attestations
	// we don't suspect byzantine behaviour, so source is not shuffled and we try to pull from
	// the same source again, it should try for MaxPullBlockRetries and then return a nil block.

	bf := cluster.BlockFetcher{}

	source_names := []string{"node1", "node2"}
	source_name_selector := 0
	getNameForSource := func() string {
		name := source_names[source_name_selector]
		source_name_selector = (source_name_selector + 1) % len(source_names)
		return name
	}

	bf.BlockSourceFactory = func(c cluster.FetcherConfig) cluster.BlockSource {
		node_name := getNameForSource()

		if node_name == "node1" {
			// When blocksource is used for first( or odd number) time,
			// it should return this block source
			bf.Logger.Infof("Block puller Created")
			bs := &mocks.BlockSource{}

			// return nil after 12 sec ( more than bf.Config.FetchTimeout)
			// to test if the endpoint is bft.
			bs.On("PullBlock", mock.Anything).Return(nil).After(time.Second * 12)

			bs.On("UpdateEndpoints", mock.Anything)
			bs.On("Close", mock.Anything)
			return bs
		}

		// When blocksource is used for second(or even number) time,
		// it should return this block source
		// the block puller below doesn't withold blocks
		bf.Logger.Infof("Block puller Created")
		bs := &mocks.BlockSource{}
		// Use the endpoint name as data for the block
		data := [][]byte{[]byte(node_name)}
		bs.On("PullBlock", mock.Anything).Return(&common.Block{
			Header:   &common.BlockHeader{Number: 1},
			Data:     &common.BlockData{Data: data},
			Metadata: &common.BlockMetadata{},
		}).After(time.Second * 2)

		bs.On("UpdateEndpoints", mock.Anything)
		bs.On("Close", mock.Anything)
		return bs
	}

	bf.AttestationSourceFactory = func(c cluster.FetcherConfig) cluster.AttestationSource {
		if c.Endpoints[0].Endpoint == "localhost:5100" || c.Endpoints[0].Endpoint == "localhost:5101" || c.Endpoints[0].Endpoint == "localhost:5102" || c.Endpoints[0].Endpoint == "localhost:5103" {
			// withhold block
			bf.Logger.Infof("Block puller 0 Created")
			bs := &mocks.AttestationSource{}

			// return nil after 12 sec ( more than bf.Config.FetchTimeout)
			// to test if the endpoint is bft.
			bs.On("PullAttestation", mock.Anything).Return(func(s uint64) *orderer.BlockAttestation {
				return nil
			}, func(s uint64) error {
				return nil
			}).After(time.Second * 12)
			bs.On("Close", mock.Anything)
			return bs
		}
		// the block puller below doesn't withold blocks
		bf.Logger.Infof("Attestation puller Created")
		bs := &mocks.AttestationSource{}
		attestation := &orderer.BlockAttestation{
			Header:   &common.BlockHeader{Number: 1},
			Metadata: &common.BlockMetadata{},
		}
		bs.On("PullAttestation", mock.Anything).Return(func(s uint64) *orderer.BlockAttestation {
			return attestation
		}, func(s uint64) error {
			return nil
		}).After(time.Second * 2)

		bs.On("UpdateEndpoints", mock.Anything)
		bs.On("Close", mock.Anything)
		return bs
	}

	// Inject Time Function
	bf.TimeNow = time.Now
	bf.ShuffleTimeoutThrehold = 50
	// Disable ShuffleTimeout
	bf.MaxPullBlockRetries = 2
	bf.MaxByzantineNodes = 3
	bf.ShuffleTimeout = time.Duration(0)
	bf.Logger = flogging.MustGetLogger("test")
	bf.Config.FetchTimeout = time.Duration(time.Second * 10)

	bf.Config.Endpoints = []cluster.EndpointCriteria{
		{Endpoint: "localhost:5100"}, {Endpoint: "localhost:5101"}, {Endpoint: "localhost:5102"}, {Endpoint: "localhost:5103"}, {Endpoint: "localhost:5104"}, {Endpoint: "localhost:5105"}, {Endpoint: "localhost:5106"}, {Endpoint: "localhost:5107"}, {Endpoint: "localhost:5108"},
	}

	bf.ConfirmByzantineBehavior = func(b []*orderer.BlockAttestation) bool {
		// for all cases
		return false
	}

	require.Equal(t, (*common.Block)(nil), bf.PullBlock(1))
}

type attestationServer struct {
	deliverServer
	attestationResponses  chan *orderer.BlockAttestationResponse
	delayResponse         bool
	delayResponseDuration time.Duration // used
}

func (ds *attestationServer) enqueueResponse(seq uint64) {
	select {
	case ds.attestationResponses <- &orderer.BlockAttestationResponse{Type: &orderer.BlockAttestationResponse_BlockAttestation{BlockAttestation: &orderer.BlockAttestation{
		Header: &common.BlockHeader{Number: seq},
		Metadata: &common.BlockMetadata{
			Metadata: bytes.Split([]byte("dummy metadata here"), []byte(" ")),
		},
	}}}:
	case <-ds.done:
	}
}

func (ds *attestationServer) BlockAttestations(in *common.Envelope, stream orderer.BlockAttestations_BlockAttestationsServer) error {
	if ds.delayResponse {
		select {
		case <-time.After(ds.delayResponseDuration):
		case <-ds.done:
			return nil
		}
	}

	var response *orderer.BlockAttestationResponse
	select {
	case response = <-ds.attestationResponses:
	case <-ds.done:
		return nil
	default:
		resp := orderer.BlockAttestationResponse_BlockAttestation{
			BlockAttestation: &orderer.BlockAttestation{
				Header:   &common.BlockHeader{Number: 1},
				Metadata: &common.BlockMetadata{},
			},
		}
		response = &orderer.BlockAttestationResponse{Type: &resp}
	}

	if err := stream.Send(response); err != nil {
		return err
	}

	return nil
}

func newClusterNodeWithAttestationRPC(t *testing.T) *attestationServer {
	srv, err := comm.NewGRPCServer("127.0.0.1:0", comm.ServerConfig{})
	require.NoError(t, err)
	// ds :=
	as := &attestationServer{
		deliverServer: deliverServer{
			logger:         flogging.MustGetLogger("test.debug"),
			t:              t,
			seekAssertions: make(chan func(*orderer.SeekInfo, string), 100),
			blockResponses: make(chan *orderer.DeliverResponse, 100),
			done:           make(chan struct{}),
			srv:            srv,
		},
		attestationResponses: make(chan *orderer.BlockAttestationResponse, 1),
	}
	// orderer.RegisterAtomicBroadcastServer(srv.Server(), ds)
	orderer.RegisterBlockAttestationsServer(srv.Server(), as)
	go srv.Start()
	return as
}

func newAttestationPuller(dialer *countingDialer, orderers ...string) *cluster.AttestationPuller {
	return &cluster.AttestationPuller{
		Logger: flogging.MustGetLogger("test"),
		Config: cluster.FetcherConfig{Channel: "mychannel", Signer: &mocks.SignerSerializer{}, TLSCert: []byte{}, Dialer: dialer, Endpoints: endpointCriteriaFromEndpoints(orderers...), FetchTimeout: time.Second * 10},
	}
}

func TestAttestationPullerBasicHappyPath(t *testing.T) {
	// Scenario: Single ordering node,
	// and the attestation puller pulls blocks 1
	osn := newClusterNodeWithAttestationRPC(t)
	defer osn.stop()

	dialer := newCountingDialer()
	ap := newAttestationPuller(dialer, osn.srv.Address())

	// response  attestation with header number 1
	osn.enqueueResponse(1)

	attestation, err := ap.PullAttestation(1)
	require.Equal(t, nil, err)
	require.Equal(t, uint64(1), attestation.Header.Number)
	ap.Close()
	dialer.assertAllConnectionsClosed(t)
}

func TestAttestationPullerPullAttestations(t *testing.T) {
	// Scenario: 3 ordering nodes,
	// and the attestation puller pulls attestaions for sequence number 5
	osn1 := newClusterNodeWithAttestationRPC(t)
	defer osn1.stop()

	osn2 := newClusterNodeWithAttestationRPC(t)
	defer osn2.stop()

	osn3 := newClusterNodeWithAttestationRPC(t)
	defer osn3.stop()

	dialer := newCountingDialer()
	osn1.enqueueResponse(5)
	osn2.enqueueResponse(5)
	osn3.enqueueResponse(5)

	orderers := []string{osn1.srv.Address(), osn2.srv.Address(), osn3.srv.Address()}
	for _, osn := range orderers {
		ap := newAttestationPuller(dialer, osn)
		ats, err := ap.PullAttestation(0)
		require.Equal(t, nil, err)
		require.Equal(t, uint64(5), ats.Header.Number)
		ap.Close()
	}
	dialer.assertAllConnectionsClosed(t)
}

func TestAttestationPullerCloseWhenPullingInProgress(t *testing.T) {
	// Scenario: 3 ordering nodes,
	// and the attestation puller pulls attestations for sequence number 5
	// attestationpuller is stopped while pulling attestations
	// orderer1 is not able to provide atetstation while orderer 2 and 3 should be able to provide
	osn1 := newClusterNodeWithAttestationRPC(t)
	// orderer node 1, is slow,  gives a response after 2 seconds
	osn1.delayResponse = true
	osn1.delayResponseDuration = time.Second * 2
	defer osn1.stop()

	osn2 := newClusterNodeWithAttestationRPC(t)
	defer osn2.stop()

	osn3 := newClusterNodeWithAttestationRPC(t)
	defer osn3.stop()

	dialer := newCountingDialer()

	osn1.enqueueResponse(5)
	osn2.enqueueResponse(5)
	osn3.enqueueResponse(5)

	orderers := []string{osn1.srv.Address(), osn2.srv.Address(), osn3.srv.Address()}
	cont := make(chan bool)

	for i, o := range orderers {
		ap := newAttestationPuller(dialer, o)
		// delay responses
		go func(c chan bool) {
			// This goroutine closes the attestation puller after 1 second
			c <- true
			<-time.After(time.Second * 1)
			ap.Close()
		}(cont)

		<-cont
		attestation, err := ap.PullAttestation(0)
		if i == 0 {
			// orderer 1 is slow, attestation pulling should fail
			require.Equal(t, true, err != nil)
		} else {
			// attestation pulling should pass for orderers 2 and 3
			require.Equal(t, true, err == nil)
			require.Equal(t, uint64(5), attestation.Header.Number)
		}
	}

	close(cont)

	dialer.assertAllConnectionsClosed(t)
}

func TestAttestationPullerCloseWhenPullingComplete(t *testing.T) {
	// Scenario: 3 ordering nodes,
	// and the attestation puller pulls attestaions for sequence number 5
	// attestationpuller is Closed after pulling attestations

	osn1 := newClusterNodeWithAttestationRPC(t)
	// orderer node 1, is slow,  gives a response after 2 seconds
	osn1.delayResponse = true
	osn1.delayResponseDuration = time.Second * 2
	defer osn1.stop()

	osn2 := newClusterNodeWithAttestationRPC(t)
	defer osn2.stop()

	osn3 := newClusterNodeWithAttestationRPC(t)
	defer osn3.stop()

	dialer := newCountingDialer()

	osn1.enqueueResponse(5)
	osn2.enqueueResponse(5)
	osn3.enqueueResponse(5)

	orderers := []string{osn1.srv.Address(), osn2.srv.Address(), osn3.srv.Address()}
	cont := make(chan bool)

	for i, o := range orderers {
		ap := newAttestationPuller(dialer, o)
		// delay responses
		go func(c chan bool) {
			// This goroutine closes the attestation puller after 1 second
			c <- true
			<-time.After(time.Second * 5)
			ap.Close()
		}(cont)

		<-cont
		attestation, err := ap.PullAttestation(0)
		if i == 0 {
			// orderer 1 is slow but close is called after 5 seconds
			// attestation pulling should pass for orderers 1,2 and 3
			require.Equal(t, true, err == nil)
			require.Equal(t, uint64(5), attestation.Header.Number)
		}
	}

	close(cont)
	dialer.assertAllConnectionsClosed(t)
}

func TestAttestationPullerPullAttestationsEmptyEndpoint(t *testing.T) {
	// Scenario: 1 ordering node
	// and the attestation puller tries to pulls attestaions
	// when node address is not set in attestation puller

	osn1 := newClusterNodeWithAttestationRPC(t)
	defer osn1.stop()

	dialer := newCountingDialer()
	osn1.enqueueResponse(5)

	// no endpoints set for attestationPuller
	ap := newAttestationPuller(dialer)
	ats, err := ap.PullAttestation(0)

	// it should return a nil atetstation and an error
	require.Equal(t, true, err != nil)
	require.Equal(t, (*orderer.BlockAttestation)(nil), ats)

	ap.Close()
	dialer.assertAllConnectionsClosed(t)
}

type failingDialer struct {
	countingDialer
}

func (d *failingDialer) Dial(address cluster.EndpointCriteria) (*grpc.ClientConn, error) {
	return nil, errors.New("failed to dial connection")
}

func newFailingCountingDialer() *failingDialer {
	builder := balancer.Get(roundrobin.Name)

	buff := make([]byte, 16)
	rand.Read(buff)
	cb := countingDialer{
		name:        hex.EncodeToString(buff),
		baseBuilder: builder,
	}

	balancer.Register(&cb)
	fd := &failingDialer{countingDialer: cb}

	return fd
}

func TestAttestationPullerPullAttestationsDialFailure(t *testing.T) {
	osn := newClusterNodeWithAttestationRPC(t)
	defer osn.stop()

	dialer := newFailingCountingDialer()
	ap := &cluster.AttestationPuller{
		Logger: flogging.MustGetLogger("test"),
		Config: cluster.FetcherConfig{
			Channel: "mychannel",
			Signer:  &mocks.SignerSerializer{}, TLSCert: []byte{},
			Dialer:       dialer,
			Endpoints:    endpointCriteriaFromEndpoints(osn.srv.Address()),
			FetchTimeout: time.Second * 10,
		},
	}
	// response  attestation with header number 1
	osn.enqueueResponse(1)

	_, err := ap.PullAttestation(1)
	require.NotEqual(t, nil, err)
	ap.Close()
	dialer.assertAllConnectionsClosed(t)
}
