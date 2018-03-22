package memory

import (
	"errors"

	hcache "github.com/hashicorp/golang-lru"
	"github.com/sdstack/storage/cache"
)

type config struct {
	Size int
}

type CacheLRU struct {
	cfg *config
	c   *hcache.Cache
}

func init() {
	cache.RegisterCache("memory-lru", &CacheLRU{})
}

func (c *CacheLRU) Configure(data interface{}) error {
	//	var err error
	/*
		err = mapstructure.Decode(data, &c.cfg)
		if err != nil {
			return err
		}
	*/
	c.cfg = &config{Size: data.(int)}
	return nil
}

func (c *CacheLRU) OnEvict(onEvict func(interface{}, interface{})) error {
	var err error
	c.c, err = hcache.NewWithEvict(c.cfg.Size, onEvict)
	return err
}

func (c *CacheLRU) Peek(k interface{}) (interface{}, bool) {
	return c.c.Peek(k)
}

func (c *CacheLRU) Purge() error {
	c.c.Purge()
	return nil
}

func (c *CacheLRU) Size() int {
	return c.c.Len()
}

func (c *CacheLRU) Set(k interface{}, v interface{}) error {
	if c.c.Add(k, v) {
		return nil
	}
	return errors.New("failed")
}

func (c *CacheLRU) Del(k interface{}) error {
	c.c.Remove(k)
	return nil
}

func (c *CacheLRU) Get(k interface{}) (interface{}, error) {
	v, ok := c.c.Get(k)
	if ok {
		return v, nil
	}
	return v, errors.New("failed")
}

func (c *CacheLRU) Keys() ([]interface{}, error) {
	v := c.c.Keys()
	return v, nil
}

func (c *CacheLRU) Exists(k interface{}) (bool, error) {
	return c.c.Contains(k), nil
}
