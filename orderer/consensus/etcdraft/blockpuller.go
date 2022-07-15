/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package etcdraft

import (
	"encoding/pem"
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/pkg/errors"
)

const (
	// compute endpoint shuffle timeout from FetchTimeout
	// endpoint shuffle timeout should be shuffleTimeoutMultiplier times FetchTimeout
	shuffleTimeoutMultiplier = 10
	// timeout expressed as a percentage of FetchtimeOut
	// if PullBlock returns before (shuffleTimeoutPercentage% of FetchTimeOut of blockfetcher)
	// the source is shuffled.
	shuffleTimeoutPercentage = int64(50)
)

// LedgerBlockPuller pulls blocks upon demand, or fetches them from the ledger
type LedgerBlockPuller struct {
	BlockPuller
	BlockRetriever cluster.BlockRetriever
	Height         func() uint64
}

func (lp *LedgerBlockPuller) PullBlock(seq uint64) *common.Block {
	lastSeq := lp.Height() - 1
	if lastSeq >= seq {
		return lp.BlockRetriever.Block(seq)
	}
	return lp.BlockPuller.PullBlock(seq)
}

// EndpointconfigFromSupport extracts TLS CA certificates and endpoints from the ConsenterSupport
func EndpointconfigFromSupport(support consensus.ConsenterSupport, bccsp bccsp.BCCSP) ([]cluster.EndpointCriteria, error) {
	lastConfigBlock, err := lastConfigBlockFromSupport(support)
	if err != nil {
		return nil, err
	}
	endpointconf, err := cluster.EndpointconfigFromConfigBlock(lastConfigBlock, bccsp)
	if err != nil {
		return nil, err
	}
	return endpointconf, nil
}

func lastConfigBlockFromSupport(support consensus.ConsenterSupport) (*common.Block, error) {
	lastBlockSeq := support.Height() - 1
	lastBlock := support.Block(lastBlockSeq)
	if lastBlock == nil {
		return nil, errors.Errorf("unable to retrieve block [%d]", lastBlockSeq)
	}
	lastConfigBlock, err := cluster.LastConfigBlock(lastBlock, support)
	if err != nil {
		return nil, err
	}
	return lastConfigBlock, nil
}

// NewBlockPuller creates a new block puller
func NewBlockPuller(support consensus.ConsenterSupport,
	baseDialer *cluster.PredicateDialer,
	clusterConfig localconfig.Cluster,
	bccsp bccsp.BCCSP,
) (BlockPuller, error) {
	verifyBlockSequence := func(blocks []*common.Block, _ string) error {
		return cluster.VerifyBlocks(blocks, support)
	}

	stdDialer := &cluster.StandardDialer{
		Config: baseDialer.Config,
	}
	stdDialer.Config.AsyncConnect = false
	stdDialer.Config.SecOpts.VerifyCertificate = nil

	// Extract the TLS CA certs and endpoints from the configuration,
	endpoints, err := EndpointconfigFromSupport(support, bccsp)
	if err != nil {
		return nil, err
	}

	der, _ := pem.Decode(stdDialer.Config.SecOpts.Certificate)
	if der == nil {
		return nil, errors.Errorf("client certificate isn't in PEM format: %v",
			string(stdDialer.Config.SecOpts.Certificate))
	}

	bp := &cluster.BlockPuller{
		VerifyBlockSequence: verifyBlockSequence,
		Logger:              flogging.MustGetLogger("orderer.common.cluster.puller").With("channel", support.ChannelID()),
		RetryTimeout:        clusterConfig.ReplicationRetryTimeout,
		MaxTotalBufferBytes: clusterConfig.ReplicationBufferSize,
		FetchTimeout:        clusterConfig.ReplicationPullTimeout,
		Endpoints:           endpoints,
		Signer:              support,
		TLSCert:             der.Bytes,
		Channel:             support.ChannelID(),
		Dialer:              stdDialer,
		StopChannel:         make(chan struct{}),
	}

	return &LedgerBlockPuller{
		Height:         support.Height,
		BlockRetriever: support,
		BlockPuller:    bp,
	}, nil
}

// NewBlockFetcher creates a new block fetcher
func NewBlockFetcher(support consensus.ConsenterSupport,
	baseDialer *cluster.PredicateDialer,
	clusterConfig localconfig.Cluster,
	bccsp bccsp.BCCSP,
) (BlockPuller, error) {
	verifyBlockSequence := func(blocks []*common.Block, _ string) error {
		return cluster.VerifyBlocks(blocks, support)
	}

	stdDialer := &cluster.StandardDialer{
		Config: baseDialer.Config,
	}
	stdDialer.Config.AsyncConnect = false
	stdDialer.Config.SecOpts.VerifyCertificate = nil

	// Extract the TLS CA certs and endpoints from the configuration,
	endpoints, err := EndpointconfigFromSupport(support, bccsp)
	if err != nil {
		return nil, err
	}

	der, _ := pem.Decode(stdDialer.Config.SecOpts.Certificate)
	if der == nil {
		return nil, errors.Errorf("client certificate isn't in PEM format: %v",
			string(stdDialer.Config.SecOpts.Certificate))
	}

	// TODO: change this to use the new mapping of consenters in the channel config

	fc := cluster.FetcherConfig{
		Channel:                      support.ChannelID(),
		TLSCert:                      der.Bytes,
		Endpoints:                    endpoints,
		FetchTimeout:                 clusterConfig.ReplicationPullTimeout,
		CensorshipSuspicionThreshold: time.Duration((int64(clusterConfig.ReplicationPullTimeout) * shuffleTimeoutPercentage / 100)),
		PeriodicalShuffleInterval:    shuffleTimeoutMultiplier * clusterConfig.ReplicationPullTimeout,
		MaxRetries:                   uint64(clusterConfig.ReplicationMaxRetries),
	}

	bf_logger := flogging.MustGetLogger("orderer.common.cluster.puller").With("channel", support.ChannelID())

	lastConfigBlock, err := lastConfigBlockFromSupport(support)
	if err != nil {
		return nil, err
	}

	verifierFactory := cluster.BlockVerifierBuilder(bccsp)

	bf := cluster.BlockFetcher{
		FetcherConfig:        fc,
		LastConfigBlock:      lastConfigBlock,
		BlockVerifierFactory: verifierFactory,
		VerifyBlock:          verifierFactory(lastConfigBlock),
		AttestationSourceFactory: func(c cluster.FetcherConfig, latestConfigBlock *common.Block) cluster.AttestationSource {
			fc, err := cluster.UpdateFetcherConfigFromConfigBlock(c, latestConfigBlock)
			bf_logger.Errorf("Could not update FetcherConfig fom Config Block: %v", err)
			return &cluster.AttestationPuller{
				Config: fc,
				Logger: flogging.MustGetLogger("orderer.common.cluster.attestationpuller").With("channel", fc.Channel),
			}
		},
		BlockSourceFactory: func(c cluster.FetcherConfig, latestConfigBlock *common.Block) cluster.BlockSource {
			// update FetcherConfig from latestConfigBlock
			fc, err := cluster.UpdateFetcherConfigFromConfigBlock(c, latestConfigBlock)
			bf_logger.Errorf("Could not update FetcherConfig fom Config Block: %v", err)
			return &cluster.BlockPuller{
				VerifyBlockSequence: verifyBlockSequence,
				Logger:              flogging.MustGetLogger("orderer.common.cluster.puller").With("channel", fc.Channel),
				RetryTimeout:        clusterConfig.ReplicationRetryTimeout,
				MaxTotalBufferBytes: clusterConfig.ReplicationBufferSize,
				FetchTimeout:        clusterConfig.ReplicationPullTimeout,
				Endpoints:           fc.Endpoints,
				Signer:              support,
				TLSCert:             der.Bytes,
				Channel:             fc.Channel,
				Dialer:              stdDialer,
				StopChannel:         make(chan struct{}),
			}
		},
		Logger:  bf_logger,
		Signer:  support,
		Dialer:  stdDialer,
		TimeNow: time.Now,
	}

	return &LedgerBlockPuller{
		Height:         support.Height,
		BlockRetriever: support,
		BlockPuller:    &bf,
	}, nil
}
