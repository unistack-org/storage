package backend

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

/*
You could do "files" as variable length blocks, where you have a list of blocks needed to reconstruct. To insert data you create a new block, keep the original, but specify that you only need parts of the data.
A file is a list of blocks. When you insert new data, you create a new block, and adjust start/end of affected blocks.
This allows you to insert data at the same speed as end-of-file writes. You can add some lazy cleanup, that removes unused parts of blocks and recompresses them, but the important part is that you do not need to do that when the file is updated.
There will only be a read overhead when "start" of a block is >0. That will be the same for using "ReadAt". You skip in your blocks until you get to the position you want and start decoding from there.
If blocks are stored based on their uncompressed hash, you furthermore get a coarse high-level deduplication, though that of course makes it more difficult to figure out when a block is no longer used by any file.
With that, you should get pretty good performance, even with block sizes up to 16-64MB.
For each block, I would probably go with variably sized de-duplication, with a 4K average, and deflate the result @level 1. That should give you ~50-60MB/core throughput. If you need higher write throughput, you can always set deflate to level 0 (store), and leave it up to a lazy task to compress the data. That should give you about 150MB/core. Obviously you can de-duplicate/compress several blocks in parallel.
*/

/*
type File struct {
	Name   string
	Size   uint64
	Blocks []Block
	Ring   *config.Ring
}

type Block struct {
	Start uint64
	Stop  uint64
	Size  uint64
}
*/

var (
	ErrIO = errors.New("IO error")
)

var backendTypes map[string]Backend

func init() {
	backendTypes = make(map[string]Backend)
}

func RegisterBackend(engine string, backend Backend) {
	backendTypes[engine] = backend
}

func New(btype string, cfg interface{}) (Backend, error) {
	var err error

	backend, ok := backendTypes[btype]
	if !ok {
		return nil, fmt.Errorf("unknown backend type %s. only %s supported", btype, strings.Join(BackendTypes(), ","))
	}

	if cfg == nil {
		return backend, nil
	}

	err = backend.Init(cfg)
	if err != nil {
		return nil, err
	}

	return backend, nil
}

func BackendTypes() []string {
	var btypes []string
	for btype, _ := range backendTypes {
		btypes = append(btypes, btype)
	}
	return btypes
}

type Backend interface {
	Configure(interface{}) error
	Init(interface{}) error
	ReaderFrom(string, io.Reader, int64, int64, int, int) (int64, error)
	WriterTo(string, io.Writer, int64, int64, int, int) (int64, error)
	WriteAt(string, []byte, int64, int, int) (int, error)
	ReadAt(string, []byte, int64, int, int) (int, error)
	Allocate(string, int64, int, int) error
	Remove(string, int, int) error
	Exists(string, int, int) (bool, error)
}
