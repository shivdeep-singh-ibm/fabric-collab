/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package genesisconfig

const (
	// The type key for etcd based RAFT consensus.
	EtcdRaft = "etcdraft"
)

type Kafka struct {
	Brokers []string `yaml:"Brokers"`
}
