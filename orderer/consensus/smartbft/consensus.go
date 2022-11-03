/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package smartbft

import (
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/consensus"
)

var logger = flogging.MustGetLogger("orderer.consensus.solo")

type consenter struct{}

func (c consenter) IsChannelMember(joinBlock *common.Block) (bool, error) {
	return true, nil
}

func (c consenter) RemoveInactiveChainRegistry() {
}

type chain struct {
	support consensus.ConsenterSupport
}

// New creates a new consenter for the smartbft consensus scheme.
func New() consensus.Consenter {
	return &consenter{}
}

func (solo *consenter) HandleChain(support consensus.ConsenterSupport, metadata *common.Metadata) (consensus.Chain, error) {
	logger.Warningf("dummy smartbft chain.")
	return newChain(support), nil
}

func newChain(support consensus.ConsenterSupport) *chain {
	return &chain{
		support: support,
	}
}

func (ch *chain) Start() {
}

func (ch *chain) Halt() {
}

func (ch *chain) WaitReady() error {
	return nil
}

// Order accepts normal messages for ordering
func (ch *chain) Order(env *common.Envelope, configSeq uint64) error {
	return nil
}

// Configure accepts configuration update messages for ordering
func (ch *chain) Configure(config *common.Envelope, configSeq uint64) error {
	return nil
}

// Errored only closes on exit
func (ch *chain) Errored() <-chan struct{} {
	return make(<-chan struct{})
}
