package etcdint

import (
	"errors"
	"time"

	"github.com/sdstack/storage/cluster"

	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/wal"
	"github.com/mitchellh/mapstructure"
)

type config struct {
	WalSize int64  `mapstructure:"wal_size"`
	Store   string `mapstructure:"store"`
}

// Internal strect holds data used by internal cluster engine
type ClusterEtcdint struct {
	etcdsrv *embed.Etcd
	etcdcfg *embed.Config
	cfg     *config
}

func init() {
	cluster.RegisterCluster("etcdint", &ClusterEtcdint{})
}

func (c *ClusterEtcdint) Configure(data interface{}) error {
	var err error

	c.etcdcfg = embed.NewConfig()
	err = mapstructure.Decode(data, &c.cfg)
	if err != nil {
		return err
	}
	c.etcdcfg.Dir = c.cfg.Store

	return nil
}

// Start internal cluster engine
func (c *ClusterEtcdint) Start() error {
	wal.SegmentSizeBytes = c.cfg.WalSize
	e, err := embed.StartEtcd(c.etcdcfg)
	if err != nil {
		return err
	}
	select {
	case <-e.Server.ReadyNotify():
		c.etcdsrv = e
		break
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		return errors.New("Server took too long to start!")
	}
	return nil
}

// Stop internal cluster engin
func (c *ClusterEtcdint) Stop() error {
	c.etcdsrv.Server.Stop()
	return nil
}
