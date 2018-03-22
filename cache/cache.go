package cache

import (
	"fmt"
	"strings"
)

type Cache interface {
	Configure(interface{}) error
	Set(interface{}, interface{}) error
	Exists(interface{}) (bool, error)
	Get(interface{}) (interface{}, error)
	Keys() ([]interface{}, error)
	//Size() int
	Purge() error
	Del(interface{}) error
	OnEvict(func(interface{}, interface{})) error
}

var cacheTypes map[string]Cache

func init() {
	cacheTypes = make(map[string]Cache)
	RegisterCache("", &Dummy{})
}

func RegisterCache(engine string, cache Cache) {
	cacheTypes[engine] = cache
}

func New(ctype string, cfg interface{}) (Cache, error) {
	var err error

	cache, ok := cacheTypes[ctype]
	if !ok {
		return nil, fmt.Errorf("unknown cluster type %s. only %s supported", ctype, strings.Join(CacheTypes(), ","))
	}

	if cfg == nil {
		return cache, nil
	}

	err = cache.Configure(cfg)
	if err != nil {
		return nil, err
	}

	return cache, nil
}

func CacheTypes() []string {
	var ctypes []string
	for ctype, _ := range cacheTypes {
		ctypes = append(ctypes, ctype)
	}
	return ctypes
}

type Dummy struct{}

func (*Dummy) Configure(interface{}) error {
	return nil
}

func (*Dummy) Del(interface{}) error {
	return nil
}

func (*Dummy) Set(interface{}, interface{}) error {
	return nil
}
func (*Dummy) Get(interface{}) (interface{}, error) {
	return nil, nil
}

func (*Dummy) Exists(interface{}) (bool, error) {
	return false, nil
}

func (*Dummy) Keys() ([]interface{}, error) {
	return nil, nil
}

func (*Dummy) OnEvict(func(interface{}, interface{})) error {
	return nil
}

func (*Dummy) Peek(interface{}) (interface{}, error) {
	return nil, nil
}

func (*Dummy) Purge() error {
	return nil
}

func (*Dummy) Size() int {
	return 0
}
