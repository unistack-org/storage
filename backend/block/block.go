package block

/*
import (
	"fmt"
	"os"

	"github.com/mitchellh/mapstructure"
	"golang.org/x/sys/unix"
	"github.com/sdstack/storage/backend"
)

type StoreBlock struct {
	store []string
}

type ObjectBlock struct {
	fs *StoreBlock
	fp *os.File
}

func init() {
	backend.RegisterBackend("block", &StoreBlock{})
}
func (s *StoreBlock) Init(data interface{}) error {
	var err error

	err = mapstructure.Decode(data, &s.store)
	if err != nil {
		return err
	}
	fmt.Printf("%#+v\n", s)
	return nil
}

func (s *StoreBlock) Configure(data interface{}) error {
	var err error

	err = mapstructure.Decode(data, &s.store)
	if err != nil {
		return err
	}
	fmt.Printf("%#+v\n", s)
	return nil
}

func (s *StoreBlock) Open() error {
	return nil
}

func (s *StoreBlock) Close() error {
	return nil
}

func (o *ObjectBlock) Allocate(mode uint64, offset uint64, length uint64) error {
	return unix.Fallocate(int(o.fp.Fd()), uint32(mode), int64(offset), int64(length))
}

func (o *ObjectBlock) Delete(flags uint64) error {
	return nil
	//	return os.Remove(filepath.Join())
}

func (o *ObjectBlock) ReadAt(data []byte, offset uint64, flags uint64) (uint64, error) {
	return 0, nil
}

func (o *ObjectBlock) WriteAt(data []byte, offset uint64, flags uint64) (uint64, error) {
	return 0, nil
}

func (o *ObjectBlock) Read(data []byte) (int, error) {
	return o.fp.Read(data)
}

func (o *ObjectBlock) Write(data []byte) (int, error) {
	return o.fp.Write(data)
}

func (o *ObjectBlock) Seek(offset int64, whence int) (int64, error) {
	return o.fp.Seek(offset, whence)
}

func (o *StoreBlock) ObjectExists(name string) (bool, error) {
	_, err := os.Stat(name)
	return os.IsNotExist(err), nil
}

func (o *ObjectBlock) Sync() error {
	return o.fp.Sync()
}

func (o *ObjectBlock) Close() error {
	return o.fp.Close()
}

func (o *ObjectBlock) Remove() error {
	return nil
}
*/
