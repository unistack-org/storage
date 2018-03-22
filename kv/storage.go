package kv

import (
	"fmt"
	"io"

	"github.com/sdstack/storage/backend"
	"github.com/sdstack/storage/cache"
	"github.com/sdstack/storage/cluster"
)

type KV struct {
	backend backend.Backend
	cluster cluster.Cluster
	cache   cache.Cache
}

type Cluster struct {
	Engine string
	Config interface{}
}

type Store struct {
	Engine string
	Config interface{}
}

func New(cfg interface{} /* c cluster.Cluster, b backend.Backend */) (*KV, error) {
	var err error

	/*
		clusterEngine := viper.GetStringMap("cluster")["engine"].(string)
		cluster, err := cluster.New(clusterEngine, viper.GetStringMap("cluster")[clusterEngine])
		if err != nil {
			return nil, err
		}

		if err = cluster.Start(); err != nil {
			log.Printf("cluster start error %s", err)
			os.Exit(1)
		}
		defer cluster.Stop()

		return &KV{backend: b, cluster: c}, nil
	*/
	return &KV{}, err
}

func (e *KV) SetBackend(b backend.Backend) error {
	if e.backend != nil {
		return fmt.Errorf("backend already set")
	}
	e.backend = b

	return nil
}

func (e *KV) SetCluster(c cluster.Cluster) error {
	if e.cluster != nil {
		return fmt.Errorf("cluster already set")
	}
	e.cluster = c

	return nil
}

func (e *KV) SetCache(c cache.Cache) {
	e.cache = c
}

func (e *KV) Exists(s string, ndata int, nparity int) (bool, error) {
	return e.backend.Exists(s, ndata, nparity)
}

func (e *KV) Allocate(name string, size int64, ndata int, nparity int) error {
	return e.backend.Allocate(name, size, ndata, nparity)
}

func (e *KV) Remove(name string, ndata int, nparity int) error {
	return e.backend.Remove(name, ndata, nparity)
}

func (e *KV) WriteAt(name string, buf []byte, offset int64, ndata int, nparity int) (int, error) {
	return e.backend.WriteAt(name, buf, offset, ndata, nparity)
}

func (e *KV) ReadAt(name string, buf []byte, offset int64, ndata int, nparity int) (int, error) {
	return e.backend.ReadAt(name, buf, offset, ndata, nparity)
}

type RW struct {
	Name    string
	KV      *KV
	Offset  int64
	Size    int64
	Ndata   int
	Nparity int
}

func (e *RW) Seek(offset int64, whence int) (int64, error) {
	e.Offset = offset
	return offset, nil
}

func (e *RW) Read(buf []byte) (int, error) {
	return e.KV.backend.ReadAt(e.Name, buf, e.Offset, e.Ndata, e.Nparity)
}

func (e *RW) ReaderFrom(r io.Reader) (int64, error) {
	return e.KV.backend.ReaderFrom(e.Name, r, e.Offset, e.Size, e.Ndata, e.Nparity)
}

func (e *RW) Write(buf []byte) (int, error) {
	return e.KV.backend.WriteAt(e.Name, buf, e.Offset, e.Ndata, e.Nparity)
}

func (e *RW) WriterTo(w io.Writer) (int64, error) {
	return e.KV.backend.WriterTo(e.Name, w, e.Offset, e.Size, e.Ndata, e.Nparity)
}
