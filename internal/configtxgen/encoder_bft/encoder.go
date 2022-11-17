/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package encoder_bft

import (
	"fmt"
	"io/ioutil"

	cb "github.com/hyperledger/fabric-protos-go/common"
	mspa "github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/policydsl"
	ec "github.com/hyperledger/fabric/internal/configtxgen/encoder"
	"github.com/hyperledger/fabric/internal/configtxgen/genesisconfig"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

const (
	ordererAdminsPolicyName = "/Channel/Orderer/Admins"

	msgVersion = int32(0)
	epoch      = 0
)

var (
	addValue                = ec.AddValue
	AddPolicies             = ec.AddPolicies
	AddOrdererPolicies      = ec.AddOrdererPolicies
	DefaultConfigTemplate   = ec.DefaultConfigTemplate
	ConfigTemplateFromGroup = ec.ConfigTemplateFromGroup
	HasSkippedForeignOrgs   = ec.HasSkippedForeignOrgs
)

var logger = flogging.MustGetLogger("common.tools.configtxgen.encoder")

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

// NewChannelGroup defines the root of the channel configuration.  It defines basic operating principles like the hashing
// algorithm used for the blocks, as well as the location of the ordering service.  It will recursively call into the
// NewOrdererGroup, NewConsortiumsGroup, and NewApplicationGroup depending on whether these sub-elements are set in the
// configuration.  All mod_policy values are set to "Admins" for this group, with the exception of the OrdererAddresses
// value which is set to "/Channel/Orderer/Admins".
func NewChannelGroup(conf *genesisconfig.Profile) (*cb.ConfigGroup, error) {
	channelGroup, err := ec.NewChannelGroup(conf)
	if conf.Orderer != nil {
		// shiva
		// return error if not bft
		if conf.Orderer.OrdererType != ConsensusTypeBFT {
			return nil, errors.Wrap(err, "could not create orderer group. ConsensusType is not BFT")
		}

		channelGroup.Groups[channelconfig.OrdererGroupKey], err = NewOrdererGroup(conf.Orderer)
		if err != nil {
			return nil, errors.Wrap(err, "could not create orderer group")
		}
	}
	return channelGroup, nil
}

// NewOrdererGroup returns the orderer component of the channel configuration.  It defines parameters of the ordering service
// about how large blocks should be, how frequently they should be emitted, etc. as well as the organizations of the ordering network.
// It sets the mod_policy of all elements to "Admins".  This group is always present in any channel configuration.
func NewOrdererGroup(conf *genesisconfig.Orderer) (*cb.ConfigGroup, error) {
	ordererGroup, err := ec.NewOrdererGroup(conf)
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

func consenterProtosFromConfig(consenterMapping []*genesisconfig.Consenter) ([]*cb.Consenter, error) {
	var consenterProtos []*cb.Consenter
	for _, consenter := range consenterMapping {
		c := &cb.Consenter{
			Id:    consenter.ID,
			Host:  consenter.Host,
			Port:  consenter.Port,
			MspId: consenter.MSPID,
		}
		// Expect the user to set the config value for client/server certs or identity to the
		// path where they are persisted locally, then load these files to memory.
		if consenter.ClientTLSCert != "" {
			clientCert, err := ioutil.ReadFile(consenter.ClientTLSCert)
			if err != nil {
				return nil, fmt.Errorf("cannot load client cert for consenter %s:%d: %s", c.GetHost(), c.GetPort(), err)
			}
			c.ClientTlsCert = clientCert
		}

		if consenter.ServerTLSCert != "" {
			serverCert, err := ioutil.ReadFile(consenter.ServerTLSCert)
			if err != nil {
				return nil, fmt.Errorf("cannot load server cert for consenter %s:%d: %s", c.GetHost(), c.GetPort(), err)
			}
			c.ServerTlsCert = serverCert
		}

		if consenter.Identity != "" {
			identity, err := ioutil.ReadFile(consenter.Identity)
			if err != nil {
				return nil, fmt.Errorf("cannot load identity for consenter %s:%d: %s", c.GetHost(), c.GetPort(), err)
			}
			c.ServerTlsCert = identity
		}

		consenterProtos = append(consenterProtos, c)
	}
	return consenterProtos, nil
}

// NewConsortiumsGroup returns an org component of the channel configuration.  It defines the crypto material for the
// organization (its MSP).  It sets the mod_policy of all elements to "Admins".
func NewConsortiumOrgGroup(conf *genesisconfig.Organization) (*cb.ConfigGroup, error) {
	return ec.NewConsortiumOrgGroup(conf)
}

// NewOrdererOrgGroup returns an orderer org component of the channel configuration.  It defines the crypto material for the
// organization (its MSP).  It sets the mod_policy of all elements to "Admins".
func NewOrdererOrgGroup(conf *genesisconfig.Organization) (*cb.ConfigGroup, error) {
	return ec.NewOrdererOrgGroup(conf)
}

// NewApplicationGroup returns the application component of the channel configuration.  It defines the organizations which are involved
// in application logic like chaincodes, and how these members may interact with the orderer.  It sets the mod_policy of all elements to "Admins".
func NewApplicationGroup(conf *genesisconfig.Application) (*cb.ConfigGroup, error) {
	return ec.NewApplicationGroup(conf)
}

// NewApplicationOrgGroup returns an application org component of the channel configuration.  It defines the crypto material for the organization
// (its MSP) as well as its anchor peers for use by the gossip network.  It sets the mod_policy of all elements to "Admins".
func NewApplicationOrgGroup(conf *genesisconfig.Organization) (*cb.ConfigGroup, error) {
	return ec.NewApplicationOrgGroup(conf)
}

// NewConsortiumsGroup returns the consortiums component of the channel configuration.  This element is only defined for the ordering system channel.
// It sets the mod_policy for all elements to "/Channel/Orderer/Admins".
func NewConsortiumsGroup(conf map[string]*genesisconfig.Consortium) (*cb.ConfigGroup, error) {
	return ec.NewConsortiumsGroup(conf)
}

// NewConsortiums returns a consortiums component of the channel configuration.  Each consortium defines the organizations which may be involved in channel
// creation, as well as the channel creation policy the orderer checks at channel creation time to authorize the action.  It sets the mod_policy of all
// elements to "/Channel/Orderer/Admins".
func NewConsortiumGroup(conf *genesisconfig.Consortium) (*cb.ConfigGroup, error) {
	return ec.NewConsortiumGroup(conf)
}

// NewChannelCreateConfigUpdate generates a ConfigUpdate which can be sent to the orderer to create a new channel.  Optionally, the channel group of the
// ordering system channel may be passed in, and the resulting ConfigUpdate will extract the appropriate versions from this file.
func NewChannelCreateConfigUpdate(channelID string, conf *genesisconfig.Profile, templateConfig *cb.ConfigGroup) (*cb.ConfigUpdate, error) {
	return ec.NewChannelCreateConfigUpdate(channelID, conf, templateConfig)
}

// MakeChannelCreationTransaction is a handy utility function for creating transactions for channel creation.
// It assumes the invoker has no system channel context so ignores all but the application section.
func MakeChannelCreationTransaction(
	channelID string,
	signer identity.SignerSerializer,
	conf *genesisconfig.Profile,
) (*cb.Envelope, error) {
	return ec.MakeChannelCreationTransaction(channelID, signer, conf)
}

// MakeChannelCreationTransactionWithSystemChannelContext is a utility function for creating channel creation txes.
// It requires a configuration representing the orderer system channel to allow more sophisticated channel creation
// transactions modifying pieces of the configuration like the orderer set.
func MakeChannelCreationTransactionWithSystemChannelContext(
	channelID string,
	signer identity.SignerSerializer,
	conf,
	systemChannelConf *genesisconfig.Profile,
) (*cb.Envelope, error) {
	return ec.MakeChannelCreationTransactionWithSystemChannelContext(channelID, signer, conf, systemChannelConf)
}

type Bootstrapper = ec.Bootstrapper

// NewBootstrapper creates a bootstrapper but returns an error instead of panic-ing
func NewBootstrapper(config *genesisconfig.Profile) (*Bootstrapper, error) {
	return ec.NewBootstrapper(config)
}

// New creates a new Bootstrapper for generating genesis blocks
func New(config *genesisconfig.Profile) *Bootstrapper {
	return ec.New(config)
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
