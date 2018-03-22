package filesystem

import (
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/mitchellh/mapstructure"
	"github.com/valyala/fastrand"
	"golang.org/x/sys/unix"
	"github.com/sdstack/storage/backend"
	"github.com/sdstack/storage/cache"
	shash "github.com/sdstack/storage/hash"
	"github.com/sdstack/storage/ring"
)

type BackendFilesystem struct {
	cfg     *config
	hash    hash.Hash
	fdcache cache.Cache
	rng     fastrand.RNG
	mu      sync.Mutex
}

const (
	vSize = 16 * 1024 * 1024
)

type config struct {
	Debug   bool
	FDCache struct {
		Engine string
		Size   int
	} `mapstructure:"fdcache"`
	Mode    string `mapstructure:"mode"`
	Options map[string]interface{}
	Shards  struct {
		Data   int
		Parity int
	}
	Ring  string
	Store []struct {
		ID     string
		Path   string
		Weight int
	}
}

func init() {
	s := &BackendFilesystem{}
	s.weights = make(map[string]int)
	backend.RegisterBackend("filesystem", s)
}

func (s *BackendFilesystem) Init(data interface{}) error {
	var err error
	var fi os.FileInfo
	var statfs unix.Statfs_t

	err = mapstructure.Decode(data, &s.cfg)
	if err != nil {
		return err
	}

	for _, it := range s.cfg.Store {
		if it.Weight < 0 {
			continue
		}

		if fi, err = os.Stat(it.Path); err != nil || !fi.IsDir() {
			continue
		}

		if it.Weight > 0 {
			s.weights[it.Path] = it.Weight
			continue
		}

		if err = unix.Statfs(it.Path, &statfs); err == nil {
			s.weights[it.Path] = int(statfs.Blocks) * int(statfs.Bsize) / vSize
		}
	}

	if s.cfg.Mode == "copy" && len(s.weights) < s.cfg.Shards.Data {
		return fmt.Errorf("data shards is more then available disks")
	}
	/*
		if s.fdcache, err = cache.New(s.cfg.FDCache.Engine, s.cfg.FDCache.Size); err != nil {
			return err
		}

		if err = s.fdcache.OnEvict(s.onCacheEvict); err != nil {
			return err
		}
	*/
	if s.ring, err = ring.New(s.cfg.Ring, s.weights); err != nil {
		return err
	}

	if s.hash, err = shash.New("xxhash"); err != nil {
		return err
	}

	return nil
}

/*
func (s *StoreFilesystem) onCacheEvict(k interface{}, v interface{}) {
	v.(*os.File).Close()
}
*/

func (s *BackendFilesystem) Configure(data interface{}) error {
	var err error
	var fi os.FileInfo

	err = mapstructure.Decode(data, &s.cfg)
	if err != nil {
		return err
	}

	for _, it := range s.cfg.Store {
		if fi, err = os.Stat(it.Path); err != nil || !fi.IsDir() {
			continue
		}
		s.weights[it.Path] = it.Weight
	}

	if s.cfg.Mode == "copy" && len(s.weights) < s.cfg.Shards.Data {
		return fmt.Errorf("data shards is more then available disks")
	}

	s.ring, err = ring.New(s.cfg.Ring, s.weights)

	s.fdcache.Purge()
	return err
}

func (s *BackendFilesystem) Allocate(name string, size int64, ndata int, nparity int) error {
	var err error
	var fp *os.File
	var items interface{}
	var disks []string

	if s.cfg.Debug {
		fmt.Printf("%T %s\n", s, "allocate")
	}

	if nparity == 0 && ndata > 0 {
		if items, err = s.ring.GetItem(name, ndata); err != nil {
			return err
		}
	}

	disks = items.([]string)
	if s.cfg.Debug {
		fmt.Printf("%T %s %v %s\n", s, "read", disks, name)
	}

	var errs []error
	for _, disk := range disks {
		if fp, err = os.OpenFile(filepath.Join(disk, name), os.O_RDWR|os.O_CREATE, os.FileMode(0660)); err != nil {
			continue
			// TODO: remove from ring
		}
		if err = unix.Fallocate(int(fp.Fd()), 0, 0, int64(size)); err != nil {
			// TODO: remove from ring
			fp.Close()
		}
	}

	if len(errs) > 0 {
		return errs[0]
	}

	return nil
}

func (s *BackendFilesystem) ReadAt(name string, buf []byte, offset int64, ndata int, nparity int) (int, error) {
	if s.cfg.Debug {
		fmt.Printf("%T %s\n", s, "read")
	}
	//var errs []error
	var err error
	var n int
	var items interface{}
	var disks []string
	var fp *os.File

	if nparity == 0 && ndata > 0 {
		if items, err = s.ring.GetItem(name, ndata); err != nil {
			return 0, err
		}
	}

	disks = items.([]string)
	if s.cfg.Debug {
		fmt.Printf("%T %s %v %s %d %d\n", s, "read", disks, name, offset, len(buf))
	}

	for len(disks) > 0 {
		s.mu.Lock()
		disk := disks[int(s.rng.Uint32n(uint32(len(disks))))]
		s.mu.Unlock()
		fname := filepath.Join(disk, name)
		if fp, err = os.OpenFile(fname, os.O_CREATE|os.O_RDWR, os.FileMode(0660)); err != nil {
			continue
		}
		n, err = fp.ReadAt(buf, int64(offset))
		fp.Close()
		if err == nil || err == io.EOF {
			//fmt.Printf("ret from read %d %s\n", n, buf)
			return n, nil
		}
		//		fmt.Printf("aaaa %v\n", err)
		/*
			o.fs.ring.DelItem(disk)
			o.fs.ring.SetState(ring.StateDegrade)

			copy(disks[rnd:], disks[rnd+1:])
			//		disks[len(disks)-1] = nil
			disks = disks[:len(disks)-1]
		*/
	}

	return 0, backend.ErrIO
}

type rwres struct {
	n   int
	err error
}

func (s *BackendFilesystem) WriterTo(name string, w io.Writer, offset int64, size int64, ndata int, nparity int) (int64, error) {

	buf := make([]byte, size)
	n, err := s.ReadAt(name, buf, offset, ndata, nparity)
	if err != nil || int64(n) < size {
		return 0, backend.ErrIO
	}
	n, err = w.Write(buf)
	return int64(n), err
}

func (s *BackendFilesystem) ReaderFrom(name string, r io.Reader, offset int64, size int64, ndata int, nparity int) (int64, error) {
	buf := make([]byte, size)
	n, err := r.Read(buf)
	if err != nil || int64(n) < size {
		return 0, backend.ErrIO
	}
	n, err = s.WriteAt(name, buf, offset, ndata, nparity)
	return int64(n), err
}

func (s *BackendFilesystem) WriteAt(name string, buf []byte, offset int64, ndata int, nparity int) (int, error) {
	if s.cfg.Debug {
		fmt.Printf("%T %s\n", s, "write")
	}

	var n int
	var err error
	var items interface{}
	var disks []string
	var fp *os.File
	if nparity == 0 && ndata > 0 {
		if items, err = s.ring.GetItem(name, ndata); err != nil {
			return 0, err
		}
	}
	disks = items.([]string)
	hinfo := struct {
		Hashes []uint64
	}{}
	_ = hinfo

	//result := make(chan rwres, len(disks))
	for _, disk := range disks {
		//		go func() {
		//var res rwres

		fp, err = os.OpenFile(filepath.Join(disk, name), os.O_CREATE|os.O_RDWR, os.FileMode(0660))
		if err != nil {
			continue
		}

		//mw := io.MultiWriter{s.hash, fp
		n, err = fp.WriteAt(buf, int64(offset))
		/*
			if o.fs.cfg.Options.Sync {
				if res.err = fp.Sync(); res.err != nil {
					result <- res
				}
			}
		*/
		fp.Close()
	}

	if s.ring.Size() < 1 {
		s.ring.SetState(ring.StateFail)
		return 0, fmt.Errorf("can't write to failed ring")
	}
	/*
		if res.err != nil || res.n < len(buf) {
			n = res.n
			err = res.err
			//delete(o.fps, res.disk)
		}
	*/
	return n, nil
}

func (s *BackendFilesystem) Exists(name string, ndata int, nparity int) (bool, error) {
	if s.cfg.Debug {
		fmt.Printf("%T %s %s\n", s, "object_exists", name)
	}

	var err error
	var items interface{}
	var disks []string
	if nparity == 0 && ndata > 0 {
		if items, err = s.ring.GetItem(name, ndata); err != nil {
			return false, err
		}
	}
	disks = items.([]string)

	for len(disks) > 0 {
		s.mu.Lock()
		disk := disks[int(s.rng.Uint32n(uint32(len(disks))))]
		s.mu.Unlock()
		if _, err = os.Stat(filepath.Join(disk, name)); err == nil {
			break
		}
	}

	return !os.IsNotExist(err), nil
}

func (s *BackendFilesystem) Remove(name string, ndata int, nparity int) error {
	if s.cfg.Debug {
		fmt.Printf("%T %s\n", s, "remove")
	}

	return nil
}
