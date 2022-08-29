/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package replication

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/msp"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	mspconstants "github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// StandardDialer wraps an ClientConfig, and provides
// a means to connect according to given EndpointCriteria.
type StandardDialer struct {
	Config comm.ClientConfig
}

// Dial dials an address according to the given EndpointCriteria
func (dialer *StandardDialer) Dial(endpointCriteria EndpointCriteria) (*grpc.ClientConn, error) {
	clientConfigCopy := dialer.Config
	clientConfigCopy.SecOpts.ServerRootCAs = endpointCriteria.TLSRootCAs

	return clientConfigCopy.Dial(endpointCriteria.Endpoint)
}

// BlockSequenceVerifier verifies that the given consecutive sequence
// of blocks is valid.
type BlockSequenceVerifier func(blocks []*common.Block, channel string) error

// Dialer creates a gRPC connection to a remote address
type Dialer interface {
	Dial(endpointCriteria EndpointCriteria) (*grpc.ClientConn, error)
}

// EndpointCriteria defines criteria of how to connect to a remote orderer node.
type EndpointCriteria struct {
	Endpoint   string   // Endpoint of the form host:port
	TLSRootCAs [][]byte // PEM encoded TLS root CA certificates
}

// String returns a string representation of this EndpointCriteria
func (ep EndpointCriteria) String() string {
	var formattedCAs []interface{}
	for _, rawCAFile := range ep.TLSRootCAs {
		var bl *pem.Block
		pemContent := rawCAFile
		for {
			bl, pemContent = pem.Decode(pemContent)
			if bl == nil {
				break
			}
			cert, err := x509.ParseCertificate(bl.Bytes)
			if err != nil {
				break
			}

			issuedBy := cert.Issuer.String()
			if cert.Issuer.String() == cert.Subject.String() {
				issuedBy = "self"
			}

			info := make(map[string]interface{})
			info["Expired"] = time.Now().After(cert.NotAfter)
			info["Subject"] = cert.Subject.String()
			info["Issuer"] = issuedBy
			formattedCAs = append(formattedCAs, info)
		}
	}

	formattedEndpointCriteria := make(map[string]interface{})
	formattedEndpointCriteria["Endpoint"] = ep.Endpoint
	formattedEndpointCriteria["CAs"] = formattedCAs

	rawJSON, err := json.Marshal(formattedEndpointCriteria)
	if err != nil {
		return fmt.Sprintf("{\"Endpoint\": \"%s\"}", ep.Endpoint)
	}

	return string(rawJSON)
}

// EndpointconfigFromConfigBlock retrieves TLS CA certificates and endpoints
// from a config block.
func EndpointconfigFromConfigBlock(block *common.Block, bccsp bccsp.BCCSP) ([]EndpointCriteria, error) {
	if block == nil {
		return nil, errors.New("nil block")
	}
	envelopeConfig, err := protoutil.ExtractEnvelope(block, 0)
	if err != nil {
		return nil, err
	}

	bundle, err := channelconfig.NewBundleFromEnvelope(envelopeConfig, bccsp)
	if err != nil {
		return nil, errors.Wrap(err, "failed extracting bundle from envelope")
	}
	msps, err := bundle.MSPManager().GetMSPs()
	if err != nil {
		return nil, errors.Wrap(err, "failed obtaining MSPs from MSPManager")
	}
	ordererConfig, ok := bundle.OrdererConfig()
	if !ok {
		return nil, errors.New("failed obtaining orderer config from bundle")
	}

	mspIDsToCACerts := make(map[string][][]byte)
	var aggregatedTLSCerts [][]byte
	for _, org := range ordererConfig.Organizations() {
		// Validate that every orderer org has a corresponding MSP instance in the MSP Manager.
		msp, exists := msps[org.MSPID()]
		if !exists {
			return nil, errors.Errorf("no MSP found for MSP with ID of %s", org.MSPID())
		}

		// Build a per org mapping of the TLS CA certs for this org,
		// and aggregate all TLS CA certs into aggregatedTLSCerts to be used later on.
		var caCerts [][]byte
		caCerts = append(caCerts, msp.GetTLSIntermediateCerts()...)
		caCerts = append(caCerts, msp.GetTLSRootCerts()...)
		mspIDsToCACerts[org.MSPID()] = caCerts
		aggregatedTLSCerts = append(aggregatedTLSCerts, caCerts...)
	}

	endpointsPerOrg := perOrgEndpoints(ordererConfig, mspIDsToCACerts)
	if len(endpointsPerOrg) > 0 {
		return endpointsPerOrg, nil
	}

	return globalEndpointsFromConfig(aggregatedTLSCerts, bundle), nil
}

func perOrgEndpoints(ordererConfig channelconfig.Orderer, mspIDsToCerts map[string][][]byte) []EndpointCriteria {
	var endpointsPerOrg []EndpointCriteria

	for _, org := range ordererConfig.Organizations() {
		for _, endpoint := range org.Endpoints() {
			endpointsPerOrg = append(endpointsPerOrg, EndpointCriteria{
				TLSRootCAs: mspIDsToCerts[org.MSPID()],
				Endpoint:   endpoint,
			})
		}
	}

	return endpointsPerOrg
}

func globalEndpointsFromConfig(aggregatedTLSCerts [][]byte, bundle *channelconfig.Bundle) []EndpointCriteria {
	var globalEndpoints []EndpointCriteria
	for _, endpoint := range bundle.ChannelConfig().OrdererAddresses() {
		globalEndpoints = append(globalEndpoints, EndpointCriteria{
			Endpoint:   endpoint,
			TLSRootCAs: aggregatedTLSCerts,
		})
	}
	return globalEndpoints
}

// ErrSkipped denotes that replicating a chain was skipped
var ErrSkipped = errors.New("skipped")

// ErrForbidden denotes that an ordering node refuses sending blocks due to access control.
var ErrForbidden = errors.New("forbidden pulling the channel")

// ErrServiceUnavailable denotes that an ordering node is not servicing at the moment.
var ErrServiceUnavailable = errors.New("service unavailable")

// ErrNotInChannel denotes that an ordering node is not in the channel
var ErrNotInChannel = errors.New("not in the channel")

var ErrRetryCountExhausted = errors.New("retry attempts exhausted")

func bundleFromConfigBlock(block *common.Block, bccsp bccsp.BCCSP) (*channelconfig.Bundle, error) {
	if block.Data == nil || len(block.Data.Data) == 0 {
		return nil, errors.New("block contains no data")
	}

	env := &common.Envelope{}
	if err := proto.Unmarshal(block.Data.Data[0], env); err != nil {
		return nil, err
	}

	bundle, err := channelconfig.NewBundleFromEnvelope(env, bccsp)
	if err != nil {
		return nil, err
	}

	return bundle, nil
}

// getConsentersAndPolicyFromConfigBlock returns a tuple of (bftEnabled, consenters, policy, error)
func getConsentersAndPolicyFromConfigBlock(block *common.Block, bccsp bccsp.BCCSP) (bool, []*common.Consenter, policies.Policy, error) {
	bundle, err := bundleFromConfigBlock(block, bccsp)
	if err != nil {
		return false, nil, nil, err
	}

	policy, exists := bundle.PolicyManager().GetPolicy(policies.BlockValidation)
	if !exists {
		return false, nil, nil, errors.New("no policies in config block")
	}

	bftEnabled := bundle.ChannelConfig().Capabilities().ConsensusTypeBFT()

	var consenters []*common.Consenter
	if bftEnabled {
		cfg, ok := bundle.OrdererConfig()
		if !ok {
			return false, nil, nil, errors.New("no orderer section in config block")
		}
		consenters = cfg.Consenters()
	}

	return bftEnabled, consenters, policy, nil
}

// BFTEnabledInConfig takes a config block as input and returns true if consenter type is BFT and also returns 'f', max byzantine suspected nodes
func BFTEnabledInConfig(block *common.Block, bccsp bccsp.BCCSP) (bool, int, error) {
	bftEnabled, consenters, _, err := getConsentersAndPolicyFromConfigBlock(block, bccsp)
	// in a bft setting, total consenter nodes should be atleast `3f+1`, to tolerate f failures
	f := int((len(consenters) - 1) / 3)
	return bftEnabled, f, err
}

// EndpointconfigFromConfigBlockV3 retrieves TLS CA certificates and endpoints from a config block.
// Unlike the EndpointconfigFromConfigBlockV function, it doesn't use a BCCSP and also doesn't honor global orderer addresses.
func EndpointconfigFromConfigBlockV3(block *common.Block) ([]EndpointCriteria, error) {
	if block == nil {
		return nil, errors.New("nil block")
	}

	envelope, err := protoutil.ExtractEnvelope(block, 0)
	if err != nil {
		return nil, err
	}

	// unmarshal the payload bytes
	payload, err := protoutil.UnmarshalPayload(envelope.Payload)
	if err != nil {
		return nil, err
	}

	// unmarshal the config envelope bytes
	configEnv := &common.ConfigEnvelope{}
	if err := proto.Unmarshal(payload.Data, configEnv); err != nil {
		return nil, err
	}

	if configEnv.Config == nil || configEnv.Config.ChannelGroup == nil || configEnv.Config.ChannelGroup.Groups == nil ||
		configEnv.Config.ChannelGroup.Groups[channelconfig.OrdererGroupKey] == nil || configEnv.Config.ChannelGroup.Groups[channelconfig.OrdererGroupKey].Groups == nil {
		return nil, errors.Errorf("invalid config, orderer groups is empty")
	}

	ordererGrp := configEnv.Config.ChannelGroup.Groups[channelconfig.OrdererGroupKey].Groups

	return perOrgEndpointsByMSPID(ordererGrp)
}

func createErrorFunc(err error) protoutil.BlockVerifierFunc {
	return func(_ *common.BlockHeader, _ *common.BlockMetadata) error {
		return errors.Wrap(err, "initialized with an invalid config block")
	}
}

func searchConsenterIdentityByID(consenters []*common.Consenter, identifier uint32) []byte {
	for _, consenter := range consenters {
		if consenter.Id == identifier {
			return protoutil.MarshalOrPanic(&msp.SerializedIdentity{
				Mspid:   consenter.MspId,
				IdBytes: consenter.Identity,
			})
		}
	}
	return nil
}

// perOrgEndpointsByMSPID returns the per orderer org endpoints
func perOrgEndpointsByMSPID(ordererGrp map[string]*common.ConfigGroup) ([]EndpointCriteria, error) {
	var res []EndpointCriteria

	for _, group := range ordererGrp {
		mspConfig := &msp.MSPConfig{}
		if err := proto.Unmarshal(group.Values[channelconfig.MSPKey].Value, mspConfig); err != nil {
			return nil, errors.Wrap(err, "failed parsing MSPConfig")
		}
		// Skip non fabric MSPs, they cannot be orderers.
		if mspConfig.Type != int32(mspconstants.FABRIC) {
			continue
		}

		fabricConfig := &msp.FabricMSPConfig{}
		if err := proto.Unmarshal(mspConfig.Config, fabricConfig); err != nil {
			return nil, errors.Wrap(err, "failed marshaling FabricMSPConfig")
		}

		var rootCAs [][]byte

		rootCAs = append(rootCAs, fabricConfig.TlsRootCerts...)
		rootCAs = append(rootCAs, fabricConfig.TlsIntermediateCerts...)

		if perOrgAddresses := group.Values[channelconfig.EndpointsKey]; perOrgAddresses != nil {
			ordererEndpoints := &common.OrdererAddresses{}
			if err := proto.Unmarshal(perOrgAddresses.Value, ordererEndpoints); err != nil {
				return nil, errors.Wrap(err, "failed unmarshalling orderer addresses")
			}

			for _, endpoint := range ordererEndpoints.Addresses {
				res = append(res, EndpointCriteria{
					TLSRootCAs: rootCAs,
					Endpoint:   endpoint,
				})
			}
		}
	}

	return res, nil
}
