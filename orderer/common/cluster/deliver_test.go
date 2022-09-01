/*
Copyright IBM Corp. 2018 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cluster_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/replication"
	rmock "github.com/hyperledger/fabric/common/replication/mocks"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/roundrobin"
)

// protects gRPC balancer registration
var gRPCBalancerLock = sync.Mutex{}

func init() {
	factory.InitFactories(nil)
}

// mock is also defined in common/replication
//go:generate counterfeiter -o mocks/signer_serializer.go --fake-name SignerSerializer . signerSerializer

type signerSerializer interface {
	identity.SignerSerializer
}

type wrappedBalancer struct {
	balancer.Balancer
	cd *countingDialer
}

func (wb *wrappedBalancer) Close() {
	defer atomic.AddUint32(&wb.cd.connectionCount, ^uint32(0))
	wb.Balancer.Close()
}

type countingDialer struct {
	name            string
	baseBuilder     balancer.Builder
	connectionCount uint32
}

func newCountingDialer() *countingDialer {
	gRPCBalancerLock.Lock()
	builder := balancer.Get(roundrobin.Name)
	gRPCBalancerLock.Unlock()

	buff := make([]byte, 16)
	rand.Read(buff)
	cb := &countingDialer{
		name:        hex.EncodeToString(buff),
		baseBuilder: builder,
	}

	gRPCBalancerLock.Lock()
	balancer.Register(cb)
	gRPCBalancerLock.Unlock()

	return cb
}

func (d *countingDialer) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	defer atomic.AddUint32(&d.connectionCount, 1)
	lb := d.baseBuilder.Build(cc, opts)
	return &wrappedBalancer{
		Balancer: lb,
		cd:       d,
	}
}

func (d *countingDialer) Name() string {
	return d.name
}

func (d *countingDialer) assertAllConnectionsClosed(t *testing.T) {
	timeLimit := time.Now().Add(timeout)
	for atomic.LoadUint32(&d.connectionCount) != uint32(0) && time.Now().Before(timeLimit) {
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, uint32(0), atomic.LoadUint32(&d.connectionCount))
}

func (d *countingDialer) Dial(address replication.EndpointCriteria) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	gRPCBalancerLock.Lock()
	balancer := grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingConfig": [{"%s":{}}]}`, d.name))
	gRPCBalancerLock.Unlock()
	return grpc.DialContext(ctx, address.Endpoint, grpc.WithBlock(), grpc.WithInsecure(), balancer)
}

func noopBlockVerifierf(_ []*common.Block, _ string) error {
	return nil
}

func readSeekEnvelope(stream orderer.AtomicBroadcast_DeliverServer) (*orderer.SeekInfo, string, error) {
	env, err := stream.Recv()
	if err != nil {
		return nil, "", err
	}
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return nil, "", err
	}
	seekInfo := &orderer.SeekInfo{}
	if err = proto.Unmarshal(payload.Data, seekInfo); err != nil {
		return nil, "", err
	}
	chdr := &common.ChannelHeader{}
	if err = proto.Unmarshal(payload.Header.ChannelHeader, chdr); err != nil {
		return nil, "", err
	}
	return seekInfo, chdr.ChannelId, nil
}

type deliverServer struct {
	logger *flogging.FabricLogger

	t *testing.T
	sync.Mutex
	err            error
	srv            *comm.GRPCServer
	seekAssertions chan func(*orderer.SeekInfo, string)
	blockResponses chan *orderer.DeliverResponse
	done           chan struct{}
}

func (ds *deliverServer) endpointCriteria() replication.EndpointCriteria {
	return replication.EndpointCriteria{Endpoint: ds.srv.Address()}
}

func (ds *deliverServer) isFaulty() bool {
	ds.Lock()
	defer ds.Unlock()
	return ds.err != nil
}

func (*deliverServer) Broadcast(orderer.AtomicBroadcast_BroadcastServer) error {
	panic("implement me")
}

func (ds *deliverServer) Deliver(stream orderer.AtomicBroadcast_DeliverServer) error {
	ds.Lock()
	err := ds.err
	ds.Unlock()

	if err != nil {
		return err
	}
	seekInfo, channel, err := readSeekEnvelope(stream)
	require.NoError(ds.t, err)

	// FAB-16233 This is meant to mitigate timeouts when
	// seekAssertions does not receive a value
	timer := time.NewTimer(1 * time.Minute)
	defer timer.Stop()

	select {
	case <-timer.C:
		ds.t.Fatalf("timed out waiting for seek assertions to receive a value\n")
	// Get the next seek assertion and ensure the next seek is of the expected type
	case seekAssert := <-ds.seekAssertions:
		ds.logger.Debugf("Received seekInfo: %+v", seekInfo)
		seekAssert(seekInfo, channel)
	case <-ds.done:
		return nil
	}

	if seekInfo.GetStart().GetSpecified() != nil {
		return ds.deliverBlocks(stream)
	}
	if seekInfo.GetStart().GetNewest() != nil {
		select {
		case resp := <-ds.blocks():
			if resp == nil {
				return nil
			}
			return stream.Send(resp)
		case <-ds.done:
		}
	}
	ds.t.Fatalf("expected either specified or newest seek but got %v\n", seekInfo.GetStart())
	return nil // unreachable
}

func (ds *deliverServer) deliverBlocks(stream orderer.AtomicBroadcast_DeliverServer) error {
	for {
		blockChan := ds.blocks()
		var response *orderer.DeliverResponse
		select {
		case response = <-blockChan:
		case <-ds.done:
			return nil
		}

		// A nil response is a signal from the test to close the stream.
		// This is needed to avoid reading from the block buffer, hence
		// consuming by accident a block that is tabled to be pulled
		// later in the test.
		if response == nil {
			return nil
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
}

func (ds *deliverServer) blocks() chan *orderer.DeliverResponse {
	ds.Lock()
	defer ds.Unlock()
	blockChan := ds.blockResponses
	return blockChan
}

func (ds *deliverServer) setBlocks(blocks chan *orderer.DeliverResponse) {
	ds.Lock()
	defer ds.Unlock()
	ds.blockResponses = blocks
}

func (ds *deliverServer) port() int {
	_, portStr, err := net.SplitHostPort(ds.srv.Address())
	require.NoError(ds.t, err)

	port, err := strconv.ParseInt(portStr, 10, 32)
	require.NoError(ds.t, err)
	return int(port)
}

func (ds *deliverServer) resurrect() {
	var err error
	// copy the responses channel into a fresh one
	respChan := make(chan *orderer.DeliverResponse, 100)
	for resp := range ds.blocks() {
		respChan <- resp
	}
	ds.blockResponses = respChan
	ds.done = make(chan struct{})
	ds.srv.Stop()
	// And re-create the gRPC server on that port
	ds.srv, err = comm.NewGRPCServer(fmt.Sprintf("127.0.0.1:%d", ds.port()), comm.ServerConfig{})
	require.NoError(ds.t, err)
	orderer.RegisterAtomicBroadcastServer(ds.srv.Server(), ds)
	go ds.srv.Start()
}

func (ds *deliverServer) stop() {
	ds.srv.Stop()
	close(ds.blocks())
	close(ds.done)
}

func (ds *deliverServer) enqueueResponse(seq uint64) {
	select {
	case ds.blocks() <- &orderer.DeliverResponse{
		Type: &orderer.DeliverResponse_Block{Block: protoutil.NewBlock(seq, nil)},
	}:
	case <-ds.done:
	}
}

func (ds *deliverServer) addExpectProbeAssert() {
	select {
	case ds.seekAssertions <- func(info *orderer.SeekInfo, _ string) {
		require.NotNil(ds.t, info.GetStart().GetNewest())
		require.Equal(ds.t, info.ErrorResponse, orderer.SeekInfo_BEST_EFFORT)
	}:
	case <-ds.done:
	}
}

func (ds *deliverServer) addExpectPullAssert(seq uint64) {
	select {
	case ds.seekAssertions <- func(info *orderer.SeekInfo, _ string) {
		seekPosition := info.GetStart()
		require.NotNil(ds.t, seekPosition)
		seekSpecified := seekPosition.GetSpecified()
		require.NotNil(ds.t, seekSpecified)
		require.Equal(ds.t, seq, seekSpecified.Number)
		require.Equal(ds.t, info.ErrorResponse, orderer.SeekInfo_BEST_EFFORT)
	}:
	case <-ds.done:
	}
}

func newClusterNode(t *testing.T) *deliverServer {
	srv, err := comm.NewGRPCServer("127.0.0.1:0", comm.ServerConfig{})
	require.NoError(t, err)
	ds := &deliverServer{
		logger:         flogging.MustGetLogger("test.debug"),
		t:              t,
		seekAssertions: make(chan func(*orderer.SeekInfo, string), 100),
		blockResponses: make(chan *orderer.DeliverResponse, 100),
		done:           make(chan struct{}),
		srv:            srv,
	}
	orderer.RegisterAtomicBroadcastServer(srv.Server(), ds)
	go srv.Start()
	return ds
}

func newBlockPuller(dialer *countingDialer, orderers ...string) *replication.BlockPuller {
	return &replication.BlockPuller{
		Dialer:              dialer,
		Channel:             "mychannel",
		Signer:              &rmock.SignerSerializer{},
		Endpoints:           endpointCriteriaFromEndpoints(orderers...),
		FetchTimeout:        time.Second * 10,
		MaxTotalBufferBytes: 1024 * 1024, // 1MB
		RetryTimeout:        time.Millisecond * 10,
		VerifyBlockSequence: noopBlockVerifierf,
		Logger:              flogging.MustGetLogger("test"),
		StopChannel:         make(chan struct{}),
	}
}

func endpointCriteriaFromEndpoints(orderers ...string) []replication.EndpointCriteria {
	var res []replication.EndpointCriteria
	for _, orderer := range orderers {
		res = append(res, replication.EndpointCriteria{Endpoint: orderer})
	}
	return res
}
