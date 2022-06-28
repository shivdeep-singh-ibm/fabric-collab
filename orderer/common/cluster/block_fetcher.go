/*
Copyright IBM Corp. 2022 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cluster

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type BlockSourceOps int

const (
	CURRENTSOURCE BlockSourceOps = iota
	SHUFFLESOURCE
	SUSPECTSOURCE
)

//go:generate mockery -dir . -name BlockSource -case underscore -output mocks/

type BlockSource interface {
	PullBlock(seq uint64) *common.Block
	HeightsByEndpoints() (map[string]uint64, error)
	UpdateEndpoints(endpoints []EndpointCriteria)
	Close()
}

type attestationErrorAndResponse struct {
	err  error
	resp *orderer.BlockAttestationResponse
}

type attestationConnectionInfo struct {
	stream *impatientAttestationStream
	conn   *grpc.ClientConn
}

//go:generate mockery -dir . -name AttestationSource -case underscore -output mocks/
type AttestationSource interface {
	PullAttestation(seq uint64) (*orderer.BlockAttestation, error)
	Close()
}

type suspectSet struct {
	entries []string
	max     uint64
}

func (s *suspectSet) insert(entry string) {
	// evict the first entry if the set is full
	for uint64(len(s.entries)) >= s.max && s.max != 0 {
		s.entries = s.entries[1:]
	}

	if !s.has(entry) {
		s.entries = append(s.entries, entry)
	}
}

func (s suspectSet) has(entry string) bool {
	for _, i := range s.entries {
		if i == entry {
			return true
		}
	}
	return false
}

type groupCollection struct {
	entries []EndpointCriteria
	cursor  int
	lock    sync.Mutex
}

// select_random_entries returns n entries from groupCollection
func (g *groupCollection) select_random_entries(n int) []EndpointCriteria {
	// it uses the cursor index to select the entries, to ensure that entries are unique
	// and not repeating
	nbr := []EndpointCriteria{}

	g.lock.Lock()
	defer g.lock.Unlock()

	for i := 0; i < n; i++ {
		// select the next entry, circularly looping over the array
		k := g.entries[(g.cursor+i)%len(g.entries)]
		nbr = append(nbr, k)
	}
	g.cursor += n
	return nbr
}

// AttestationPuller pulls attestation blocks from remote ordering nodes.
// Its operations are not thread safe.
type AttestationPuller struct {
	Config FetcherConfig
	// Internal state
	openConnections []attestationConnectionInfo
	cancelStream    func()
	Logger          *flogging.FabricLogger
	lock            sync.Mutex
}

func (p *AttestationPuller) seekNextEnvelope(startSeq uint64) (*common.Envelope, error) {
	return protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		p.Config.Channel,
		p.Config.Signer,
		nextSeekInfo(startSeq),
		int32(0),
		uint64(0),
		util.ComputeSHA256(p.Config.TLSCert),
	)
}

func (p *AttestationPuller) closeEndpoints() {
	p.lock.Lock()
	defer p.lock.Unlock()
	// Close the streams and grpc connections
	for _, oc := range p.openConnections {
		oc.stream.cancelFunc()
		if oc.conn != nil {
			oc.conn.Close()
		}
	}
	p.openConnections = nil
}

// Close closes the attestation puller connections.
func (p *AttestationPuller) Close() {
	p.closeEndpoints()
}

func (p *AttestationPuller) pullAttestationBlock(ec EndpointCriteria, env *common.Envelope) (*orderer.BlockAttestation, error) {
	// The connection should be via closed via Close() method of AttestationPuller
	conn, err := p.Config.Dialer.Dial(ec)
	if err != nil {
		p.Logger.Errorf("Failed to Dial [%s]: [%v]", ec.Endpoint, err)
		return nil, err
	}

	p.Logger.Debugf("Create an impatient stream with channel:[%s] endpoint:[%s] timeout: [%v]", p.Config.Channel, ec.Endpoint, p.Config.FetchTimeout)
	stream, err := newImpatientStream(conn, p.Config.FetchTimeout, env)
	if err != nil {
		p.Logger.Errorf("Failed to create stream: [%v]", err)
		return nil, err
	}

	// append the stream to bf.openConnections, so that Close can close all streams
	// dial connections
	cni := attestationConnectionInfo{stream: stream, conn: conn}
	p.lock.Lock()
	p.openConnections = append(p.openConnections, cni)
	p.lock.Unlock()
	p.Logger.Debugf("Add grpc conn to openConnections list.")
	resp, err := stream.Recv()
	if err != nil {
		p.Logger.Warningf("Received %v from %s: %v", resp, ec.Endpoint, err)
		p.Logger.Errorf("Failed receiving next block from %s: %v", ec.Endpoint, err)
		return nil, err
	}

	defer stream.abort()

	stream.CloseSend()
	return extractAttestationFromResponse(resp)
}

func extractAttestationFromResponse(resp *orderer.BlockAttestationResponse) (*orderer.BlockAttestation, error) {
	switch resp.Type.(type) {
	case *orderer.BlockAttestationResponse_BlockAttestation:
		attestation := resp.GetBlockAttestation()
		if attestation == nil {
			return nil, errors.New("attestation block is nil")
		}
		if attestation.Header == nil {
			return nil, errors.New("attestation block header is nil")
		}
		if attestation.Metadata == nil || len(attestation.Metadata.Metadata) == 0 {
			return nil, errors.New("attestation block metadata is empty")
		}
		return attestation, nil
	case *orderer.BlockAttestationResponse_Status:
		status := resp.GetStatus()
		if status == common.Status_FORBIDDEN {
			return nil, ErrForbidden
		}
		if status == common.Status_SERVICE_UNAVAILABLE {
			return nil, ErrServiceUnavailable
		}
		return nil, errors.Errorf("faulty node, received: %v", resp)
	default:
		return nil, errors.Errorf("response is of type %v, but expected a block", reflect.TypeOf(resp.Type))
	}
}

// impatientAttestationStream aborts the stream if it waits for too long for a message.
type impatientAttestationStream struct {
	waitTimeout time.Duration
	orderer.BlockAttestations_BlockAttestationsClient
	cancelFunc func()
}

func (stream *impatientAttestationStream) abort() {
	stream.cancelFunc()
}

// Recv blocks until a response is received from the stream or the
// timeout expires.
func (stream *impatientAttestationStream) Recv() (*orderer.BlockAttestationResponse, error) {
	// Initialize a timeout to cancel the stream when it expires
	timeout := time.NewTimer(stream.waitTimeout)
	defer timeout.Stop()

	responseChan := make(chan attestationErrorAndResponse, 1)

	// receive waitGroup ensures the goroutine below exits before
	// this function exits.
	var receive sync.WaitGroup
	receive.Add(1)
	defer receive.Wait()

	go func() {
		defer receive.Done()
		resp, err := stream.BlockAttestations_BlockAttestationsClient.Recv()
		responseChan <- attestationErrorAndResponse{err: err, resp: resp}
	}()

	select {
	case <-timeout.C:
		stream.cancelFunc()
		return nil, errors.Errorf("didn't receive a response within %v", stream.waitTimeout)
	case respAndErr := <-responseChan:
		return respAndErr.resp, respAndErr.err
	}
}

// newImpatientStream returns a ImpatientStreamCreator that creates impatientStreams.
func newImpatientStream(conn *grpc.ClientConn, waitTimeout time.Duration, in *common.Envelope) (*impatientAttestationStream, error) {
	abc := orderer.NewBlockAttestationsClient(conn)
	ctx, cancel := context.WithCancel(context.Background())

	stream, err := abc.BlockAttestations(ctx, in)
	if err != nil {
		cancel()
		return nil, err
	}

	once := &sync.Once{}
	return &impatientAttestationStream{
		waitTimeout: waitTimeout,
		// The stream might be canceled while Close() is being called, but also
		// while a timeout expires, so ensure it's only called once.
		cancelFunc: func() {
			once.Do(cancel)
		},
		BlockAttestations_BlockAttestationsClient: stream,
	}, nil
}

// PullAttestation pulls an attestation Block from an orderer endpoint
func (p *AttestationPuller) PullAttestation(seq uint64) (*orderer.BlockAttestation, error) {
	if len(p.Config.Endpoints) == 0 {
		p.Logger.Errorf("No endpoints set")
		return nil, errors.Errorf("no endpoints set")
	}

	// It supports pulling from only one endpoint at a time
	ec := p.Config.Endpoints[0]

	p.Logger.Infof("Sending request for attestation block [%d] to [%s]", seq, ec.Endpoint)
	env, err := p.seekNextEnvelope(seq)
	if err != nil {
		p.Logger.Errorf("error creating envelope: %v", err)
		return nil, err
	}

	return p.pullAttestationBlock(ec, env)
}

// FetcherConfig stores the configuration parameters needed to create a BlockFetcher
type FetcherConfig struct {
	Channel      string
	Signer       identity.SignerSerializer
	TLSCert      []byte
	Dialer       Dialer
	Endpoints    []EndpointCriteria
	FetchTimeout time.Duration
}

type TimeFunc func() time.Time

// BlockFetcher can be used to fetch blocks from orderers in a byzantine fault tolerant way.
type BlockFetcher struct {
	MaxPullBlockRetries      uint64
	AttestationSourceFactory func(c FetcherConfig) AttestationSource
	BlockSourceFactory       func(c FetcherConfig) BlockSource
	Config                   FetcherConfig
	suspects                 suspectSet // a set of bft suspected nodes.
	Logger                   *flogging.FabricLogger
	currentBlockSource       BlockSource
	currentEndpoint          EndpointCriteria
	currentSeq               uint64
	ShuffleTimeout           time.Duration
	LastShuffledAt           time.Time
	MaxByzantineNodes        uint64
	ConfirmByzantineBehavior func(attestations []*orderer.BlockAttestation) bool
	ShuffleTimeoutThrehold   int64 // expressed as %age of FetchTimeOut
	TimeNow                  TimeFunc
}

func (bf *BlockFetcher) getBlockSource(op BlockSourceOps) BlockSource {
	switch op {
	case CURRENTSOURCE:
		// set the first source or shuffle the current source
		emptyEC := EndpointCriteria{}
		if bf.isShuffleTime() || bf.currentEndpoint.Endpoint == emptyEC.Endpoint {
			bf.Logger.Debugf("CURRENTSOURCE: Shuffle Endpoint")
			bf.shuffleEndpoint()
		}
	case SHUFFLESOURCE:
		bf.Logger.Debugf("SHUFFLESOURCE: Shuffle Endpoint")
		bf.shuffleEndpoint()
	case SUSPECTSOURCE:
		blockwitheld, err := bf.isBlockWitheld(bf.currentSeq)
		if blockwitheld || err != nil {
			bf.Logger.Debugf("SUSPECTSOURCE: blockwitheld: %v, err: %v", blockwitheld, err)
			bf.Logger.Debugf("SUSPECTSOURCE: Shuffle Endpoint")
			bf.shuffleEndpoint()
		}
	}

	return bf.currentBlockSource
}

func (bf *BlockFetcher) setBlockSource(ec EndpointCriteria) {
	bf.Logger.Debugf("Set [%s] as block source", ec.Endpoint)
	// close old blocksource
	if bf.currentBlockSource != nil {
		bf.currentBlockSource.Close()
	}
	// create a local config
	config := bf.Config
	// fill only current endpoint criteria
	config.Endpoints = []EndpointCriteria{ec}
	bf.currentBlockSource = bf.BlockSourceFactory(config)
	bf.currentBlockSource.UpdateEndpoints([]EndpointCriteria{ec})
}

func (bf *BlockFetcher) shuffleEndpoint() error {
	// change the currentEndpoint and set it as blocksource
	candidates := []EndpointCriteria{}
	for _, e := range bf.Config.Endpoints {
		// don't append currentEndpoint and suspects
		if bf.suspects.has(e.Endpoint) || e.Endpoint == bf.currentEndpoint.Endpoint {
			continue
		}
		candidates = append(candidates, e)
	}
	// select a random entry from candidates
	gc := groupCollection{entries: candidates}
	bf.currentEndpoint = gc.select_random_entries(1)[0]

	bf.Logger.Debugf("Shuffled block puller endpoint. Current endpoint: [%s]", bf.currentEndpoint)
	// Update current blocksource
	bf.setBlockSource(bf.currentEndpoint)
	bf.LastShuffledAt = bf.TimeNow()
	return nil
}

func (bf *BlockFetcher) isShuffleTime() bool {
	if bf.ShuffleTimeout == time.Duration(0) {
		// It should return only false if Time based Shuffling is not enabled
		bf.Logger.Debugf("Shuffle Disabled.")
		return false
	}
	shuffle := bf.TimeNow().After(bf.LastShuffledAt.Add(bf.ShuffleTimeout))
	bf.Logger.Debugf("Shuffle Enabled. Time to shuffle %v", shuffle)
	return shuffle
}

// isEndpointByzantine connects to BlockFetcher.MaxByzantineNodes orderer nodes and pulls attestation blocks from them
// to suspect the byzantine behaiour of orderer nodes.
func (bf *BlockFetcher) isEndpointByzantine(ec EndpointCriteria, seq uint64) (bool, error) {
	bf.Logger.Infof("Checking for BFT Behavior of [%s]", ec.Endpoint)

	candidates := []EndpointCriteria{}
	for _, e := range bf.Config.Endpoints {
		// don't append currentEndpoint and suspects
		if bf.suspects.has(e.Endpoint) || e.Endpoint == bf.currentEndpoint.Endpoint {
			continue
		}
		candidates = append(candidates, e)
	}

	group := groupCollection{entries: candidates}

	wg := sync.WaitGroup{}

	// attestationsC is a channel to collect attestation blocks
	attestationsC := make(chan *orderer.BlockAttestation, bf.MaxByzantineNodes)

	wg.Add(int(bf.MaxByzantineNodes))
	for i := 0; i < int(bf.MaxByzantineNodes); i++ {
		// goroutine to pull attestations and put then in attestationsC channel
		go func(c chan *orderer.BlockAttestation) {
			defer wg.Done()
			config := bf.Config
			var attestation_puller AttestationSource

			// This `for` loop should not go infinite, so looping for maximum
			// 2 * bf.MaxByzantineNodes unless we find a unique orderer which sends attestations.
			for j := 0; j < 2*int(bf.MaxByzantineNodes); j++ {
				// Choose a random orderer node and and try to pull attestations from it.
				// if not able to pull attestation s due to error, choose a different
				// endpoint, until an endpoint is found which sends attestations.

				// choose a random orderer node
				config.Endpoints = group.select_random_entries(1)

				if attestation_puller != nil {
					bf.Logger.Errorf("Close previous attestation puller")
					attestation_puller.Close()
				}
				bf.Logger.Debugf("Create attestation source [%s]", config.Endpoints[0].Endpoint)
				attestation_puller = bf.AttestationSourceFactory(config)
				defer attestation_puller.Close()
				attestation, err := attestation_puller.PullAttestation(seq)
				if err != nil {
					bf.Logger.Errorf("Error pulling attestation from [%s]: %v", config.Endpoints[0].Endpoint, err)
					continue
				}
				c <- attestation
				break
			}
		}(attestationsC)
	}

	// wait till all goroutines return
	wg.Wait()
	// close the attestationC channel
	close(attestationsC)

	attestations := []*orderer.BlockAttestation{}
	for a := range attestationsC {
		attestations = append(attestations, a)
	}

	// if number of attestations pulled doesn't match bf.MaxByzantineNodes,
	// we can return error
	if len(attestations) != int(bf.MaxByzantineNodes) {
		// need bf.MaxByzantineNodes number of nodes to be online
		bf.Logger.Errorf("[%d] attestations pulled from [%d] nodes", len(attestations), bf.MaxByzantineNodes)
		return true, errors.Errorf("some nodes did not send attestations")
	}
	return bf.ConfirmByzantineBehavior(attestations), nil
}

func (bf *BlockFetcher) isBlockWitheld(seq uint64) (bool, error) {
	// this should keep the current endpoint as reference and try to detect if block witholding
	// takes place by probing (MaxByzantineNodes + 1) endpoints
	bf.Logger.Info("Checking for Block withholding")
	malicious, err := bf.isEndpointByzantine(bf.currentEndpoint, seq)
	if err != nil {
		return false, err
	}
	if malicious {
		bf.Logger.Debugf("Byzantine malicious behavior suspected for [%s]", bf.currentEndpoint.Endpoint)
		bf.suspects.insert(bf.currentEndpoint.Endpoint)
		return true, nil
	}
	return false, nil
}

// PullBlock pulls blocks from orderers in a BFT way.
func (bf *BlockFetcher) PullBlock(seq uint64) *common.Block {
	retriesLeft := bf.MaxPullBlockRetries

	if bf.suspects.max == 0 {
		bf.suspects = suspectSet{max: bf.MaxByzantineNodes}
	}

	blkSourceOp := CURRENTSOURCE
	bf.currentSeq = seq

	for {
		if retriesLeft == 0 && bf.MaxPullBlockRetries > 0 {
			bf.Logger.Errorf("Failed pulling block [%d]: retry count exhausted(%d)", seq, bf.MaxPullBlockRetries)
			return nil
		}

		blkSource := bf.getBlockSource(blkSourceOp)

		start_time := bf.TimeNow()
		block := blkSource.PullBlock(seq)

		if block != nil {
			return block
		}

		// endpoint might be unavailable so shuffle
		if start_time.Add(time.Duration((int64(bf.Config.FetchTimeout) * bf.ShuffleTimeoutThrehold / 100))).After(bf.TimeNow()) {
			bf.Logger.Info("Couldn't pull block within timeout. endpoint unavailable")
			blkSourceOp = SHUFFLESOURCE
			continue
		}

		// suspect blocksource for blockwitholding
		bf.Logger.Debug("Not able to pull block. Check for Byzantine behavior")
		blkSourceOp = SUSPECTSOURCE

		retriesLeft--
	}
}

// HeightsByEndpoints returns the block heights by endpoints of orderers
func (bf BlockFetcher) HeightsByEndpoints() (map[string]uint64, error) {
	return bf.BlockSourceFactory(bf.Config).HeightsByEndpoints()
}

// UpdateEndpoints assigns the new endpoints.
func (p *BlockFetcher) UpdateEndpoints(endpoints []EndpointCriteria) {
	p.Logger.Debugf("Updating endpoints: %v", endpoints)
	p.Config.Endpoints = endpoints
}

// Close closes the blocksource of blockfetcher.
func (bf BlockFetcher) Close() {
	if bf.currentBlockSource != nil {
		bf.currentBlockSource.Close()
	}
}

// Clone returns a copy of this BlockFetcher initialized
// for the given channel
func (p *BlockFetcher) Clone() *BlockFetcher {
	// Clone by value
	copy := *p

	// Reset internal state
	copy.currentEndpoint = EndpointCriteria{}
	copy.currentSeq = 0
	copy.currentBlockSource = nil
	copy.suspects = suspectSet{}
	return &copy
}
