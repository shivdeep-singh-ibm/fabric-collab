//go:build bft

/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package genesisconfig

import (
	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/hyperledger/fabric-protos-go/orderer/smartbft"
	cf "github.com/hyperledger/fabric/core/config"
	"time"
)

const (
	// The type key for BFT Smart consensus
	SmartBFT = "smartbft"
	// SampleDevModeSmartBFTProfile references the sample profile used for testing
	// the SmartBFT-based ordering service.
	SampleDevModeSmartBFTProfile = "SampleDevModeSmartBFT"
)

var genesisDefaults = TopLevel{
	Orderer: &Orderer{
		OrdererType:  "solo",
		BatchTimeout: 2 * time.Second,
		BatchSize: BatchSize{
			MaxMessageCount:   500,
			AbsoluteMaxBytes:  10 * 1024 * 1024,
			PreferredMaxBytes: 2 * 1024 * 1024,
		},
		SmartBFT: &smartbft.Options{
			RequestBatchMaxCount:      uint64(types.DefaultConfig.RequestBatchMaxCount),
			RequestBatchMaxBytes:      uint64(types.DefaultConfig.RequestBatchMaxBytes),
			RequestBatchMaxInterval:   types.DefaultConfig.RequestBatchMaxInterval.String(),
			IncomingMessageBufferSize: uint64(types.DefaultConfig.IncomingMessageBufferSize),
			RequestPoolSize:           uint64(types.DefaultConfig.RequestPoolSize),
			RequestForwardTimeout:     types.DefaultConfig.RequestForwardTimeout.String(),
			RequestComplainTimeout:    types.DefaultConfig.RequestComplainTimeout.String(),
			RequestAutoRemoveTimeout:  types.DefaultConfig.RequestAutoRemoveTimeout.String(),
			ViewChangeResendInterval:  types.DefaultConfig.ViewChangeResendInterval.String(),
			ViewChangeTimeout:         types.DefaultConfig.ViewChangeTimeout.String(),
			LeaderHeartbeatTimeout:    types.DefaultConfig.LeaderHeartbeatTimeout.String(),
			LeaderHeartbeatCount:      uint64(types.DefaultConfig.LeaderHeartbeatCount),
			CollectTimeout:            types.DefaultConfig.CollectTimeout.String(),
			SyncOnStart:               types.DefaultConfig.SyncOnStart,
			SpeedUpViewChange:         types.DefaultConfig.SpeedUpViewChange,
		},
	},
}

// Orderer contains configuration associated to a channel.
type Orderer struct {
	OrdererType      string             `yaml:"OrdererType"`
	Addresses        []string           `yaml:"Addresses"`
	BatchTimeout     time.Duration      `yaml:"BatchTimeout"`
	BatchSize        BatchSize          `yaml:"BatchSize"`
	ConsenterMapping []*Consenter       `yaml:"ConsenterMapping"`
	SmartBFT         *smartbft.Options  `yaml:"SmartBFT"`
	Organizations    []*Organization    `yaml:"Organizations"`
	MaxChannels      uint64             `yaml:"MaxChannels"`
	Capabilities     map[string]bool    `yaml:"Capabilities"`
	Policies         map[string]*Policy `yaml:"Policies"`
}

func (ord *Orderer) completeInitialization(configDir string) {
loop:
	for {
		switch {
		case ord.OrdererType == "":
			logger.Infof("Orderer.OrdererType unset, setting to %v", genesisDefaults.Orderer.OrdererType)
			ord.OrdererType = genesisDefaults.Orderer.OrdererType
		case ord.BatchTimeout == 0:
			logger.Infof("Orderer.BatchTimeout unset, setting to %s", genesisDefaults.Orderer.BatchTimeout)
			ord.BatchTimeout = genesisDefaults.Orderer.BatchTimeout
		case ord.BatchSize.MaxMessageCount == 0:
			logger.Infof("Orderer.BatchSize.MaxMessageCount unset, setting to %v", genesisDefaults.Orderer.BatchSize.MaxMessageCount)
			ord.BatchSize.MaxMessageCount = genesisDefaults.Orderer.BatchSize.MaxMessageCount
		case ord.BatchSize.AbsoluteMaxBytes == 0:
			logger.Infof("Orderer.BatchSize.AbsoluteMaxBytes unset, setting to %v", genesisDefaults.Orderer.BatchSize.AbsoluteMaxBytes)
			ord.BatchSize.AbsoluteMaxBytes = genesisDefaults.Orderer.BatchSize.AbsoluteMaxBytes
		case ord.BatchSize.PreferredMaxBytes == 0:
			logger.Infof("Orderer.BatchSize.PreferredMaxBytes unset, setting to %v", genesisDefaults.Orderer.BatchSize.PreferredMaxBytes)
			ord.BatchSize.PreferredMaxBytes = genesisDefaults.Orderer.BatchSize.PreferredMaxBytes
		default:
			break loop
		}
	}

	logger.Infof("orderer type: %s", ord.OrdererType)
	// Additional, consensus type-dependent initialization goes here
	// Also using this to panic on unknown orderer type.
	switch ord.OrdererType {
	case SmartBFT:
		if ord.SmartBFT == nil {
			logger.Infof("Orderer.SmartBFT.Options unset, setting to %v", genesisDefaults.Orderer.SmartBFT)
			ord.SmartBFT = genesisDefaults.Orderer.SmartBFT
		}

		if len(ord.ConsenterMapping) == 0 {
			logger.Panicf("%s configuration did not specify any consenter", SmartBFT)
		}

		for _, c := range ord.ConsenterMapping {
			if c.Host == "" {
				logger.Panicf("consenter info in %s configuration did not specify host", SmartBFT)
			}
			if c.Port == 0 {
				logger.Panicf("consenter info in %s configuration did not specify port", SmartBFT)
			}
			if c.ClientTLSCert == "" {
				logger.Panicf("consenter info in %s configuration did not specify client TLS cert", SmartBFT)
			}
			if c.ServerTLSCert == "" {
				logger.Panicf("consenter info in %s configuration did not specify server TLS cert", SmartBFT)
			}
			if len(c.MSPID) == 0 {
				logger.Panicf("consenter info in %s configuration did not specify MSP ID", SmartBFT)
			}
			if len(c.Identity) == 0 {
				logger.Panicf("consenter info in %s configuration did not specify identity certificate", SmartBFT)
			}

			cf.TranslatePathInPlace(configDir, &c.ClientTLSCert)
			cf.TranslatePathInPlace(configDir, &c.ServerTLSCert)
			cf.TranslatePathInPlace(configDir, &c.Identity)
		}
	default:
		logger.Panicf("unknown orderer type: %s", ord.OrdererType)
	}
}
