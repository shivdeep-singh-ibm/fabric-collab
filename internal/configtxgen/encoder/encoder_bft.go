//go:build bft

/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package encoder

import (
	cb "github.com/hyperledger/fabric-protos-go/common"
	mspa "github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/policydsl"
	"github.com/hyperledger/fabric/internal/configtxgen/genesisconfig"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

const (
	ConsensusTypeBFT = "smartbft"

	// BlockValidationPolicyKey TODO
	BlockValidationPolicyKey = "BlockValidation"

	// OrdererAdminsPolicy is the absolute path to the orderer admins policy
	OrdererAdminsPolicy = "/Channel/Orderer/Admins"

	// SignaturePolicyType is the 'Type' string for signature policies
	SignaturePolicyType = "Signature"

	// ImplicitMetaPolicyType is the 'Type' string for implicit meta policies
	ImplicitMetaPolicyType = "ImplicitMeta"
)

// NewOrdererGroup returns the orderer component of the channel configuration.  It defines parameters of the ordering service
// about how large blocks should be, how frequently they should be emitted, etc. as well as the organizations of the ordering network.
// It sets the mod_policy of all elements to "Admins".  This group is always present in any channel configuration.
func NewOrdererGroup(conf *genesisconfig.Orderer) (*cb.ConfigGroup, error) {
	ordererGroup, err := NewOrdererGroup(conf)
	if err != nil {
		return nil, err
	}

	consenterProtos, err := consenterProtosFromConfig(conf.ConsenterMapping)
	if err != nil {
		return nil, errors.Errorf("cannot load consenter config for orderer type %s: %s", ConsensusTypeBFT, err)
	}

	addValue(ordererGroup, channelconfig.OrderersValue(consenterProtos), channelconfig.AdminsPolicyKey)
	var consensusMetadata []byte
	if consensusMetadata, err = channelconfig.MarshalBFTOptions(conf.SmartBFT); err != nil {
		return nil, errors.Errorf("cannot marshal metadata for orderer type %s: %s", ConsensusTypeBFT, err)
	}
	// Overwrite policy manually by computing it from the consenters
	addBFTBlockPolicy(consenterProtos, ordererGroup)

	addValue(ordererGroup, channelconfig.ConsensusTypeValue(conf.OrdererType, consensusMetadata), channelconfig.AdminsPolicyKey)

	return ordererGroup, nil
}

func addBFTBlockPolicy(consenterProtos []*cb.Consenter, ordererGroup *cb.ConfigGroup) {
	n := len(consenterProtos)
	f := (n - 1) / 3

	var identities []*mspa.MSPPrincipal
	var pols []*cb.SignaturePolicy
	for i, consenter := range consenterProtos {
		pols = append(pols, &cb.SignaturePolicy{
			Type: &cb.SignaturePolicy_SignedBy{
				SignedBy: int32(i),
			},
		})

		//+shiva

		//crypto.SanitizeIdentity(consenter.Identity)
		//-shiva
		identities = append(identities, &mspa.MSPPrincipal{
			PrincipalClassification: mspa.MSPPrincipal_IDENTITY,
			Principal:               protoutil.MarshalOrPanic(&mspa.SerializedIdentity{Mspid: consenter.MspId, IdBytes: consenter.Identity}),
		})
	}

	sp := &cb.SignaturePolicyEnvelope{
		Rule:       policydsl.NOutOf(int32(2*f+1), pols),
		Identities: identities,
	}
	ordererGroup.Policies[BlockValidationPolicyKey] = &cb.ConfigPolicy{
		// Inherit modification policy
		ModPolicy: ordererGroup.Policies[BlockValidationPolicyKey].ModPolicy,
		Policy: &cb.Policy{
			Type:  int32(cb.Policy_SIGNATURE),
			Value: protoutil.MarshalOrPanic(sp),
		},
	}
}
