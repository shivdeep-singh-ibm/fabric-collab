/*
Copyright IBM Corp. 2022 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package replication_test

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/replication"
	"github.com/hyperledger/fabric/common/replication/mocks"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

// generates names like: node1, node2 etc on subsequent calls
func nameFactory(name string, max int) func() string {
	name_selector := 0
	return func() string {
		name_selector = (name_selector + 1) % max
		return fmt.Sprintf("%s%d", name, name_selector)
	}
}

func TestBlockFetcherHappyPath(t *testing.T) {
	t.Parallel()
	bf := replication.BlockFetcher{
		FetcherConfig: replication.FetcherConfig{
			Endpoints: []replication.EndpointCriteria{
				// add 2 endpoints to pull from
				{Endpoint: "localhost:5100"},
				{Endpoint: "localhost:5200"},
			},
			FetchTimeout:                 time.Duration(10 * time.Millisecond),
			CensorshipSuspicionThreshold: time.Duration(5 * time.Millisecond),
			// Disable shuffle timeout, shuffle interval is set to a large value to ensure no shuffling takes place
			PeriodicalShuffleInterval: time.Duration(8 * time.Hour),
			// Inject Time Function
		},
		TimeNow: time.Now,
		Logger:  flogging.MustGetLogger("test"),
		BlockSourceFactory: func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.BlockSource, error) {
			return mock_block_puller(1, nil, time.Millisecond*1), nil
		},
		BlockVerifierFactory: func(block *common.Block) protoutil.BlockVerifierFunc {
			return func(header *common.BlockHeader, metadata *common.BlockMetadata) error {
				// assuming blocks are valid
				return nil
			}
		},
	}

	bf.VerifyBlock = bf.BlockVerifierFactory(nil)

	// pullblock from any endpoint
	require.Equal(t, uint64(1), bf.PullBlock(1).Header.Number)
}

func TestBlockFetcherBlockSourceError(t *testing.T) {
	// block source factory returns error and is not able to create a block source
	// PullBlock should return nil
	t.Parallel()
	bf := replication.BlockFetcher{
		FetcherConfig: replication.FetcherConfig{
			Endpoints: []replication.EndpointCriteria{
				// add 2 endpoints to pull from
				{Endpoint: "localhost:5100"},
				{Endpoint: "localhost:5200"},
			},
			FetchTimeout:                 time.Duration(10 * time.Millisecond),
			CensorshipSuspicionThreshold: time.Duration(5 * time.Millisecond),
			// Disable shuffle timeout, shuffle interval is set to a large value to ensure no shuffling takes place
			PeriodicalShuffleInterval: time.Duration(8 * time.Hour),
			// Inject Time Function
		},
		TimeNow: time.Now,
		Logger:  flogging.MustGetLogger("test"),
		// blocksource factory should return error
		BlockSourceFactory: func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.BlockSource, error) {
			return mock_block_puller(1, nil, time.Millisecond*1), errors.New("bad config block")
		},
		BlockVerifierFactory: func(block *common.Block) protoutil.BlockVerifierFunc {
			return func(header *common.BlockHeader, metadata *common.BlockMetadata) error {
				// assuming blocks are valid
				return nil
			}
		},
	}

	var blockSourceError error
	bf.Logger = bf.Logger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Failed setting block source") {
			blockSourceError = errors.New(entry.Message)
		}
		return nil
	}))

	bf.VerifyBlock = bf.BlockVerifierFactory(nil)

	// pullblock from any endpoint
	require.Equal(t, (*common.Block)(nil), bf.PullBlock(1))
	require.NotEqual(t, nil, blockSourceError)
}

func TestBlockFetcherShuffleTimeOut(t *testing.T) {
	// node1 and node2 send blocks after 12s. Shuffle timeout is set to 12s ,so
	// successive PullBlock calls to the block puller should fetch
	// blocks from the different nodes, since the source would be shuffled after 12s
	t.Parallel()
	bf := replication.BlockFetcher{
		FetcherConfig: replication.FetcherConfig{
			Endpoints: []replication.EndpointCriteria{
				{Endpoint: "localhost:5100"},
				{Endpoint: "localhost:5200"},
				{Endpoint: "localhost:5300"},
				{Endpoint: "localhost:5400"},
				{Endpoint: "localhost:5500"},
			},
			FetchTimeout:                 time.Duration(10 * time.Millisecond),
			CensorshipSuspicionThreshold: time.Duration(5 * time.Millisecond),
			PeriodicalShuffleInterval:    time.Duration(8 * time.Millisecond),
		},
		// Inject Time Function
		TimeNow: time.Now,
		Logger:  flogging.MustGetLogger("test"),
	}

	getNameForSource := nameFactory("node", 3)

	bf.BlockVerifierFactory = func(block *common.Block) protoutil.BlockVerifierFunc {
		return func(header *common.BlockHeader, metadata *common.BlockMetadata) error {
			// assuming blocks are valid
			return nil
		}
	}
	bf.VerifyBlock = bf.BlockVerifierFactory(nil)

	// the block source created first sends blocks with data: "first"
	// the block source created seond time sends blocks with data: "second"
	// test uses this information to detetc whether blocksource has been shuffled while pulling blocks
	bf.BlockSourceFactory = func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.BlockSource, error) {
		// by default block fetcher will pull from Endpoint index 1 (not 0) for the first time
		// i.e localhost:5200
		bf.Logger.Infof("Block source get input %v", c.Endpoints)
		if c.Endpoints[0].Endpoint == "localhost:5200" {
			data := [][]byte{[]byte(getNameForSource())}
			return mock_block_puller(1, data, time.Millisecond*12), nil
		} else {
			data := [][]byte{[]byte(getNameForSource())}
			return mock_block_puller(1, data, time.Microsecond*1), nil
		}
	}

	// First block will have block.Data.Data as : []byte{"node1"}
	firstAttempt := string(bf.PullBlock(1).Data.Data[0])
	// Due to timeout, the source should have shuffled
	// Second block should be pulled from another node and have block.Data.data: []byte{"node2"}
	secondAttempt := string(bf.PullBlock(1).Data.Data[0])
	require.Equal(t, "node1", firstAttempt)
	require.Equal(t, "node2", secondAttempt)
}

func TestBlockFetcherShuffleTimeOutDisable(t *testing.T) {
	// node1 and node2 send blocks after 12s. Shuffle timeout is disabled so
	// successive PullBlock calls to the block puller should fetch
	// blocks from the same node.
	t.Parallel()
	bf := replication.BlockFetcher{
		FetcherConfig: replication.FetcherConfig{
			Endpoints: []replication.EndpointCriteria{
				{Endpoint: "localhost:5100"},
				{Endpoint: "localhost:5200"},
				{Endpoint: "localhost:5300"},
				{Endpoint: "localhost:5400"},
				{Endpoint: "localhost:5500"},
			},
			FetchTimeout:                 time.Duration(10 * time.Millisecond),
			CensorshipSuspicionThreshold: time.Duration(5 * time.Millisecond),
			// Disable shuffle timeout, shuffle interval is set to a large value to ensure no shuffling takes place
			PeriodicalShuffleInterval: time.Duration(8 * time.Hour),
		},
		// Inject Time Function
		TimeNow: time.Now,
		Logger:  flogging.MustGetLogger("test"),
	}

	getNameForSource := nameFactory("node", 3)

	bf.BlockSourceFactory = func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.BlockSource, error) {
		if c.Endpoints[0].Endpoint == "localhost:5100" {
			data := [][]byte{[]byte(getNameForSource())}
			return mock_block_puller(1, data, time.Millisecond*12), nil
		} else {
			data := [][]byte{[]byte(getNameForSource())}
			return mock_block_puller(1, data, time.Millisecond*12), nil
		}
	}

	bf.BlockVerifierFactory = func(block *common.Block) protoutil.BlockVerifierFunc {
		return func(header *common.BlockHeader, metadata *common.BlockMetadata) error {
			// assuming blocks are valid
			return nil
		}
	}

	bf.VerifyBlock = bf.BlockVerifierFactory(nil)
	// First block will have block.Data.Data as : []byte{"node1"}
	firstAttempt := string(bf.PullBlock(1).Data.Data[0])
	// the source should have not have shuffled
	// Second block should be pulled from the same node and have block.Data.data: []byte{"node1"}

	secondAttempt := string(bf.PullBlock(1).Data.Data[0])
	require.Equal(t, "node1", firstAttempt)
	require.Equal(t, "node1", secondAttempt)
}

func TestBlockFetcherNodeOffline(t *testing.T) {
	// node1 returns nil after 2s, so assuming it to be offline
	// source should be shuffled and the block should be pulled from node2
	t.Parallel()
	bf := replication.BlockFetcher{
		FetcherConfig: replication.FetcherConfig{
			Endpoints: []replication.EndpointCriteria{
				// add 2 endpoints to pull from
				{Endpoint: "localhost:5100"},
				{Endpoint: "localhost:5200"},
				{Endpoint: "localhost:5300"},
				{Endpoint: "localhost:5400"},
				{Endpoint: "localhost:5500"},
			},
			FetchTimeout:                 time.Duration(10 * time.Millisecond),
			CensorshipSuspicionThreshold: time.Duration(1 * time.Millisecond),
			// Disable shuffle timeout, shuffle interval is set to a large value to ensure no shuffling takes place
			PeriodicalShuffleInterval: time.Duration(8 * time.Hour),
			// Inject Time Function
			MaxByzantineNodes: 1,
		},
		TimeNow: time.Now,
		Logger:  flogging.MustGetLogger("test"),
	}

	getNameForSource := nameFactory("node", 3)

	bf.BlockSourceFactory = func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.BlockSource, error) {
		node_name := getNameForSource()
		if node_name == "node1" {
			return mock_block_puller_returns_nil(time.Millisecond * 2), nil
		} else {
			return mock_block_puller(2, nil, time.Millisecond*1), nil
		}
	}

	bf.BlockVerifierFactory = func(block *common.Block) protoutil.BlockVerifierFunc {
		return func(header *common.BlockHeader, metadata *common.BlockMetadata) error {
			// assuming blocks are valid
			return nil
		}
	}

	bf.VerifyBlock = bf.BlockVerifierFactory(nil)

	bf.AttestationSourceFactory = func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.AttestationSource, error) {
		// attestation source created withhold attestation
		return mock_attestation_puller(1, time.Millisecond*1), nil
	}

	// set log level to debug for this test
	flogging.ActivateSpec("debug")
	defer flogging.ActivateSpec("info")

	require.Equal(t, uint64(2), bf.PullBlock(2).Header.Number)
}

func mock_attestation_puller_returns_nil(err error, after time.Duration) replication.AttestationSource {
	bs := &mocks.AttestationSource{}

	bs.On("PullAttestation", mock.Anything).Return(func(s uint64) *orderer.BlockAttestation {
		return nil
	}, func(s uint64) error {
		return err
	}).After(after)

	bs.On("Close", mock.Anything)
	return bs
}

func mock_attestation_puller(seq uint64, after time.Duration) replication.AttestationSource {
	bs := &mocks.AttestationSource{}
	attestation := &orderer.BlockAttestation{
		Header: &common.BlockHeader{Number: seq},
		Metadata: &common.BlockMetadata{
			// dummy metadata
			Metadata: [][]byte{[]byte("signature")},
		},
	}
	bs.On("PullAttestation", mock.Anything).Return(func(s uint64) *orderer.BlockAttestation {
		return attestation
	}, func(s uint64) error {
		return nil
	}).After(after)

	bs.On("Close", mock.Anything)
	return bs
}

func mock_block_puller_returns_nil(after time.Duration) replication.BlockSource {
	bs := &mocks.BlockSource{}
	bs.On("PullBlock", mock.Anything).Return(nil).After(after)
	bs.On("UpdateEndpoints", mock.Anything)
	bs.On("Close", mock.Anything)
	return bs
}

func mock_block_puller(seq uint64, data [][]byte, after time.Duration) replication.BlockSource {
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
	t.Parallel()
	bf := replication.BlockFetcher{}

	getNameForSource := nameFactory("node", 10)

	bf.BlockSourceFactory = func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.BlockSource, error) {
		// node1 witholds block while other endpoints can deliver blocks
		node_name := getNameForSource()
		if node_name == "node1" {
			return mock_block_puller_returns_nil(time.Millisecond * 4), nil
		}
		// the block puller below doesn't withold blocks
		data := [][]byte{[]byte(node_name)}
		// this block puller returns blocks with seq:1 and data after 2s
		return mock_block_puller(1, data, time.Second*2), nil
	}

	attestation_source_created := false
	lock := sync.Mutex{}
	bf.AttestationSourceFactory = func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.AttestationSource, error) {
		lock.Lock()
		defer lock.Unlock()
		if !attestation_source_created {
			// first attestation source created withholds attestation
			attestation_source_created = true
			return mock_attestation_puller_returns_nil(nil, time.Millisecond*12), nil
		}
		// all other attestation pullers send atetstation blocks
		return mock_attestation_puller(1, time.Second*2), nil
	}

	// Add time
	bf.TimeNow = time.Now
	bf.MaxRetries = 3
	// Disable shuffle timeout
	bf.CensorshipSuspicionThreshold = time.Duration(1 * time.Millisecond)
	bf.PeriodicalShuffleInterval = time.Duration(1 * time.Hour)
	bf.MaxByzantineNodes = 3
	bf.Logger = flogging.MustGetLogger("test")
	bf.FetcherConfig.FetchTimeout = time.Duration(time.Millisecond * 3)

	bf.FetcherConfig.Endpoints = []replication.EndpointCriteria{
		{Endpoint: "localhost:5100"}, {Endpoint: "localhost:5101"}, {Endpoint: "localhost:5102"}, {Endpoint: "localhost:5103"}, {Endpoint: "localhost:5104"}, {Endpoint: "localhost:5105"}, {Endpoint: "localhost:5106"}, {Endpoint: "localhost:5107"}, {Endpoint: "localhost:5108"},
	}
	bf.BlockVerifierFactory = func(block *common.Block) protoutil.BlockVerifierFunc {
		return func(header *common.BlockHeader, metadata *common.BlockMetadata) error {
			// assuming blocks are valid
			return nil
		}
	}

	bf.VerifyBlock = bf.BlockVerifierFactory(nil)

	block := bf.PullBlock(1)
	block_data := string(block.Data.Data[0])
	require.NotEqual(t, "node1", block_data)
	require.Equal(t, uint64(1), block.Header.Number)
}

func TestBlockFetcherAttestationSourceError(t *testing.T) {
	// The first endpoint we try to pull Blocks witholds blocks,
	// the PullBlock should then suspect the endpoint and probe other endpoints to confirm the suspicion.
	// But attestation source factory fails to create attestation sources, probing should fail and
	// no block is pulled.
	t.Parallel()
	bf := replication.BlockFetcher{}

	getNameForSource := nameFactory("node", 10)

	bf.BlockSourceFactory = func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.BlockSource, error) {
		// node1 witholds block while other endpoints can deliver blocks
		node_name := getNameForSource()
		if node_name == "node1" {
			return mock_block_puller_returns_nil(time.Millisecond * 4), nil
		}
		// the block puller below doesn't withold blocks
		data := [][]byte{[]byte(node_name)}
		// this block puller returns blocks with seq:1 and data after 2s
		return mock_block_puller(1, data, time.Second*2), nil
	}

	attestation_source_created := false
	lock := sync.Mutex{}
	bf.AttestationSourceFactory = func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.AttestationSource, error) {
		lock.Lock()
		defer lock.Unlock()
		if !attestation_source_created {
			// first attestation source created withholds attestation
			attestation_source_created = true
			return mock_attestation_puller_returns_nil(nil, time.Millisecond*12), nil
		}
		// all other attestation source fail to be created
		return mock_attestation_puller(1, time.Second*2), errors.New("failed to pull attestations")
	}

	// Add time
	bf.TimeNow = time.Now
	bf.MaxRetries = 3
	// Disable shuffle timeout
	bf.CensorshipSuspicionThreshold = time.Duration(1 * time.Millisecond)
	bf.PeriodicalShuffleInterval = time.Duration(1 * time.Hour)
	bf.MaxByzantineNodes = 3
	bf.Logger = flogging.MustGetLogger("test")
	bf.FetcherConfig.FetchTimeout = time.Duration(time.Millisecond * 3)

	bf.FetcherConfig.Endpoints = []replication.EndpointCriteria{
		{Endpoint: "localhost:5100"}, {Endpoint: "localhost:5101"}, {Endpoint: "localhost:5102"}, {Endpoint: "localhost:5103"}, {Endpoint: "localhost:5104"}, {Endpoint: "localhost:5105"}, {Endpoint: "localhost:5106"}, {Endpoint: "localhost:5107"}, {Endpoint: "localhost:5108"},
	}
	bf.BlockVerifierFactory = func(block *common.Block) protoutil.BlockVerifierFunc {
		return func(header *common.BlockHeader, metadata *common.BlockMetadata) error {
			// assuming blocks are valid
			return nil
		}
	}

	bf.VerifyBlock = bf.BlockVerifierFactory(nil)
	var attestationSourceError error
	lock = sync.Mutex{}
	bf.Logger = bf.Logger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Failed to create attestation source") {
			lock.Lock()
			defer lock.Unlock()
			attestationSourceError = errors.New(entry.Message)
		}
		return nil
	}))
	block := bf.PullBlock(1)
	require.Equal(t, (*common.Block)(nil), block)
	require.NotEqual(t, nil, attestationSourceError)
}

func TestBlockFetcherBFTBehaviorSuspicionNoBlockWithhold(t *testing.T) {
	// Endpoint node1 returns nil since the block may not be present
	// PullBlock will try to probe other endpoints to suspect the endpoint localhost:5100 for witholding
	// blocks but other endpoints will also return nil, So endpoint will not be shuffled and
	// PullBlock should return nil after retries are enhausted.
	t.Parallel()
	bf := replication.BlockFetcher{}

	getNameForSource := nameFactory("node", 10)

	bf.BlockSourceFactory = func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.BlockSource, error) {
		// node1 witholds block while other endpoints can deliver blocks.

		node_name := getNameForSource()
		if node_name == "node1" {
			return mock_block_puller_returns_nil(time.Millisecond * 12), nil
		}
		// the block puller below doesn't withold blocks
		data := [][]byte{[]byte(node_name)}
		// this block puller returns blocks with seq:1 and data after 2s
		return mock_block_puller(1, data, time.Millisecond*2), nil
	}

	bf.AttestationSourceFactory = func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.AttestationSource, error) {
		// attestation source created withhold attestation
		return mock_attestation_puller_returns_nil(nil, time.Second*1), nil
	}

	// Add time
	bf.TimeNow = time.Now

	// Disable shuffle timeout
	bf.CensorshipSuspicionThreshold = time.Duration(1 * time.Millisecond)
	bf.PeriodicalShuffleInterval = time.Duration(1 * time.Hour)
	// Disable shuffle timeout
	bf.MaxRetries = 2
	bf.MaxByzantineNodes = 3
	bf.Logger = flogging.MustGetLogger("test")
	bf.FetcherConfig.FetchTimeout = time.Duration(time.Millisecond * 5)

	bf.FetcherConfig.Endpoints = []replication.EndpointCriteria{
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

	bf.BlockVerifierFactory = func(block *common.Block) protoutil.BlockVerifierFunc {
		return func(header *common.BlockHeader, metadata *common.BlockMetadata) error {
			// assuming blocks are valid
			return nil
		}
	}

	bf.VerifyBlock = bf.BlockVerifierFactory(nil)

	// No byzantine behaviour should make sure that the
	// endpoint is not shuffled.

	// node1 withholds blocks, other nodes withold attestations,
	// node1 is not suspected for byzantine behaviour
	// endpoint is not shuffled.
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
	t.Parallel()
	bf := replication.BlockFetcher{}

	source_names := []string{"node1", "node2", "node1", "node3"}
	source_name_selector := 0
	getNameForSource := func() string {
		name := source_names[source_name_selector]
		source_name_selector = (source_name_selector + 1) % len(source_names)
		return name
	}

	firstTime := true
	bf.BlockSourceFactory = func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.BlockSource, error) {
		// node1 and node 2 withold block while other endpoints can deliver blocks
		node_name := getNameForSource()
		if (node_name == "node1" && firstTime) || node_name == "node2" {
			firstTime = false
			return mock_block_puller_returns_nil(time.Second * 4), nil
		}
		// the block puller below doesn't withold blocks
		data := [][]byte{[]byte(node_name)}
		// this block puller returns blocks with seq:1 and data after 2s
		return mock_block_puller(1, data, time.Millisecond*2), nil
	}

	var lock sync.Mutex
	firstAttestationSource := true

	bf.AttestationSourceFactory = func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.AttestationSource, error) {
		lock.Lock()
		defer lock.Unlock()
		if !firstAttestationSource {
			// first attestation source created withholds attestation
			firstAttestationSource = false

			return mock_attestation_puller_returns_nil(nil, time.Millisecond*12), nil
		}

		// all other attestation pullers send atetstation blocks
		return mock_attestation_puller(1, time.Microsecond*2), nil
	}

	// Add time
	bf.TimeNow = time.Now
	// Disable shuffle timeout
	bf.CensorshipSuspicionThreshold = time.Duration(1 * time.Millisecond)
	bf.PeriodicalShuffleInterval = time.Duration(1 * time.Hour)
	// Disable shuffle timeout
	bf.MaxRetries = 5
	bf.MaxByzantineNodes = 3
	// Disable shuffle timeout
	bf.MaxByzantineNodes = 1
	bf.Logger = flogging.MustGetLogger("test")
	bf.FetcherConfig.FetchTimeout = time.Duration(time.Millisecond * 3)

	bf.FetcherConfig.Endpoints = []replication.EndpointCriteria{
		{Endpoint: "localhost:5100"}, {Endpoint: "localhost:5101"}, {Endpoint: "localhost:5102"}, {Endpoint: "localhost:5103"}, {Endpoint: "localhost:5104"}, {Endpoint: "localhost:5105"}, {Endpoint: "localhost:5106"}, {Endpoint: "localhost:5107"}, {Endpoint: "localhost:5108"},
	}

	// simulate byzantine behaviour
	bf.BlockVerifierFactory = func(block *common.Block) protoutil.BlockVerifierFunc {
		return func(header *common.BlockHeader, metadata *common.BlockMetadata) error {
			// assuming blocks are valid
			return nil
		}
	}

	bf.VerifyBlock = bf.BlockVerifierFactory(nil)

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
	t.Parallel()
	bf := replication.BlockFetcher{}

	source_names := []string{"node1", "node2", "node3", "node4", "node5", "node6", "node7", "node8", "node9", "node10"}
	source_name_selector := 0
	getNameForSource := func() string {
		name := source_names[source_name_selector]
		source_name_selector = (source_name_selector + 1) % len(source_names)
		return name
	}

	bf.BlockSourceFactory = func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.BlockSource, error) {
		// node1 witholds block while other endpoints can deliver blocks
		node_name := getNameForSource()
		if node_name == "node1" {
			return mock_block_puller_returns_nil(time.Millisecond * 4), nil
		}
		// the block puller below doesn't withold blocks
		data := [][]byte{[]byte(node_name)}
		// this block puller returns blocks with seq:1 and data after 2s
		return mock_block_puller(1, data, time.Millisecond*2), nil
	}

	attestation_source_created := false
	lock := sync.Mutex{}
	bf.AttestationSourceFactory = func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.AttestationSource, error) {
		lock.Lock()
		defer lock.Unlock()
		if !attestation_source_created {
			bf.Logger.Infof("attestation source: connection failure")
			// first attestation source created returns error
			attestation_source_created = true
			return mock_attestation_puller_returns_nil(errors.New("connection failure"), time.Millisecond*12), nil
		}
		bf.Logger.Infof("attestaion source , all good")
		// all other attestation pullers send atetstation blocks
		return mock_attestation_puller(1, time.Millisecond*2), nil
	}

	bf.TimeNow = time.Now

	bf.CensorshipSuspicionThreshold = time.Duration(1 * time.Millisecond)
	bf.PeriodicalShuffleInterval = time.Duration(1 * time.Hour)
	bf.MaxRetries = 5
	bf.MaxByzantineNodes = 3
	bf.MaxByzantineNodes = 1

	// Add time
	bf.TimeNow = time.Now
	bf.Logger = flogging.MustGetLogger("test")

	var attestationError error
	// set log level to debug for this test
	flogging.ActivateSpec("debug")
	defer flogging.ActivateSpec("info")
	// Add a hook to check for error and debug message
	bf.Logger = bf.Logger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "connection failure") {
			attestationError = errors.New(entry.Message)
		}
		return nil
	}))

	bf.FetcherConfig.FetchTimeout = time.Duration(time.Second * 3)

	bf.FetcherConfig.Endpoints = []replication.EndpointCriteria{
		{Endpoint: "localhost:5100"}, {Endpoint: "localhost:5101"}, {Endpoint: "localhost:5102"}, {Endpoint: "localhost:5103"}, {Endpoint: "localhost:5104"}, {Endpoint: "localhost:5105"}, {Endpoint: "localhost:5106"}, {Endpoint: "localhost:5107"}, {Endpoint: "localhost:5108"},
	}

	bf.BlockVerifierFactory = func(block *common.Block) protoutil.BlockVerifierFunc {
		return func(header *common.BlockHeader, metadata *common.BlockMetadata) error {
			// assuming blocks are valid
			return nil
		}
	}

	bf.VerifyBlock = bf.BlockVerifierFactory(nil)

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
	t.Parallel()

	bf := replication.BlockFetcher{
		FetcherConfig: replication.FetcherConfig{
			Endpoints: []replication.EndpointCriteria{
				{Endpoint: "localhost:5100"},
				{Endpoint: "localhost:5101"},
				{Endpoint: "localhost:5102"},
				{Endpoint: "localhost:5103"},
				{Endpoint: "localhost:5104"},
				{Endpoint: "localhost:5105"},
				{Endpoint: "localhost:5106"},
				{Endpoint: "localhost:5107"},
				{Endpoint: "localhost:5108"},
				{Endpoint: "localhost:5109"},
			},
			FetchTimeout:                 time.Duration(10 * time.Millisecond),
			CensorshipSuspicionThreshold: time.Duration(1 * time.Millisecond),
			// Disable shuffle timeout, shuffle interval is set to a large value to ensure no shuffling takes place
			PeriodicalShuffleInterval: time.Duration(8 * time.Hour),
			MaxRetries:                2,
			MaxByzantineNodes:         3,
		},
		TimeNow: time.Now,
		Logger:  flogging.MustGetLogger("test"),
	}
	source_names := []string{"node1", "node2"}
	source_name_selector := 0
	getNameForSource := func() string {
		name := source_names[source_name_selector]
		source_name_selector = (source_name_selector + 1) % len(source_names)
		return name
	}

	bf.BlockSourceFactory = func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.BlockSource, error) {
		// by default block fetcher will pull from Endpoint index 1 (not 0) for the first time
		// i.e localhost:5200
		node_name := getNameForSource()
		if node_name == "node1" {
			return mock_block_puller_returns_nil(time.Millisecond * 12), nil
			// return mock_block_puller(1, data, time.Millisecond*12)
		} else {
			data := [][]byte{[]byte(getNameForSource())}
			return mock_block_puller(1, data, time.Microsecond*1), nil
		}
	}

	bf.AttestationSourceFactory = func(c replication.FetcherConfig, latestConfigBlock *common.Block) (replication.AttestationSource, error) {
		if c.Endpoints[0].Endpoint == "localhost:5100" || c.Endpoints[0].Endpoint == "localhost:5101" || c.Endpoints[0].Endpoint == "localhost:5102" || c.Endpoints[0].Endpoint == "localhost:5103" {
			// withhold block
			return mock_attestation_puller_returns_nil(nil, time.Millisecond*12), nil
		}

		// all other attestation pullers send atetstation blocks
		return mock_attestation_puller(1, time.Second*2), nil
	}

	bf.BlockVerifierFactory = func(block *common.Block) protoutil.BlockVerifierFunc {
		return func(header *common.BlockHeader, metadata *common.BlockMetadata) error {
			// assuming blocks are invalid
			return errors.New("some error")
		}
	}
	bf.VerifyBlock = bf.BlockVerifierFactory(nil)

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

func newAttestationPuller(dialer *countingDialer, orderers ...string) *replication.AttestationPuller {
	return &replication.AttestationPuller{
		Logger: flogging.MustGetLogger("test"),
		Config: replication.FetcherConfig{
			Channel:      "mychannel",
			TLSCert:      []byte{},
			Endpoints:    endpointCriteriaFromEndpoints(orderers...),
			FetchTimeout: time.Second * 10,
		},
		Signer: &mocks.SignerSerializer{},
		Dialer: dialer,
	}
}

func TestAttestationPullerBasicHappyPath(t *testing.T) {
	// set log level to debug for this test
	// Scenario: Single ordering node,
	// and the attestation puller pulls blocks 1
	t.Parallel()
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
	t.Parallel()
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
		ats, err := ap.PullAttestation(5)
		require.Equal(t, nil, err)
		require.Equal(t, uint64(5), ats.Header.Number)
		ap.Close()
	}
	dialer.assertAllConnectionsClosed(t)
}

func TestAttestationPullerPullAttestationsEmptyEndpoint(t *testing.T) {
	// Scenario: 1 ordering node
	// and the attestation puller tries to pulls attestaions
	// when node address is not set in attestation puller
	t.Parallel()

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

func (d *failingDialer) Dial(address replication.EndpointCriteria) (*grpc.ClientConn, error) {
	return nil, errors.New("failed to dial connection")
}
