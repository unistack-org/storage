package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/sdstack/storage/cache"
)

type config struct {
	Store string
}

type LevelDB struct {
	db *leveldb.DB
}

func init() {
	cache.RegisterCache("leveldb", &LevelDB{})
}

func (c *LevelDB) Configure(cfg interface{}) error {
	return nil
}

func (c *LevelDB) Open() error {
	var err error
	c.db, err = leveldb.OpenFile("/srv/store/sc01/db", &opt.Options{
		Compression: opt.NoCompression,
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *LevelDB) Close() error {
	return c.db.Close()
}

func (c *LevelDB) Set(k interface{}, v interface{}) error {
	return c.db.Put(k.([]byte), v.([]byte), nil)
}

func (c *LevelDB) Get(k interface{}) (interface{}, error) {
	return c.db.Get(k.([]byte), nil)
}

func (c *LevelDB) Del(k interface{}) error {
	return c.db.Delete(k.([]byte), nil)
}

func (c *LevelDB) Exists(k interface{}) (bool, error) {
	return c.db.Has(k.([]byte), nil)
}

func (c *LevelDB) Keys() ([]interface{}, error) {
	var keys []interface{}
	var err error

	iter := c.db.NewIterator(nil, nil)
	for iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	iter.Release()
	err = iter.Error()
	return keys, err
}

func (c *LevelDB) OnEvict(f func(interface{}, interface{})) error {
	return nil
}

func (c *LevelDB) Purge() error {
	return nil
}
