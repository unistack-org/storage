package cluster

import (
	"fmt"
	"strings"
)

var clusterTypes map[string]Cluster

func init() {
	clusterTypes = make(map[string]Cluster)
	RegisterCluster("none", &clusterNone{})
}
func RegisterCluster(engine string, cluster Cluster) {
	clusterTypes[engine] = cluster
}

// Info struct contains cluster info
type Info struct {
	BlockSize uint32
	Mode      string
}

// Member struct contains info about cluster member
type Member struct {
	UUID    []byte
	Name    string
	Network string
	Host    string
	Port    string
}

// Cluster represents cluster interface
type Cluster interface {
	Start() error
	Stop() error
	Configure(interface{}) error
	//	Format() error
	//	Check() error
	//	Recover() error
	//	Info() (*Info, error)
	//	Snapshot() error
	//	Reweight() error
	//	Members() []Member
}

func New(ctype string, cfg interface{}) (Cluster, error) {
	var err error

	cluster, ok := clusterTypes[ctype]
	if !ok {
		return nil, fmt.Errorf("unknown cluster type %s. only %s supported", ctype, strings.Join(ClusterTypes(), ","))
	}

	if cfg == nil {
		return cluster, nil
	}

	err = cluster.Configure(cfg)
	if err != nil {
		return nil, err
	}

	return cluster, nil
}

func ClusterTypes() []string {
	var ctypes []string
	for ctype, _ := range clusterTypes {
		ctypes = append(ctypes, ctype)
	}
	return ctypes
}

type clusterNone struct{}

func (c *clusterNone) Configure(data interface{}) error {
	return nil
}

func (c *clusterNone) Start() error {
	return nil
}

func (c *clusterNone) Stop() error {
	return nil
}
