package journal

import (
	"fmt"
	"strings"
)

type Journal interface {
	Write([]byte, []byte) (int, error)
	Read([]byte, []byte) (int, error)
	Configure(interface{}) error
}

var journalTypes map[string]Journal

func init() {
	journalTypes = make(map[string]Journal)
}

func RegisterJournal(engine string, journal Journal) {
	journalTypes[engine] = journal
}

func New(ctype string, cfg interface{}) (Journal, error) {
	var err error

	journal, ok := journalTypes[ctype]
	if !ok {
		return nil, fmt.Errorf("unknown cluster type %s. only %s supported", ctype, strings.Join(JournalTypes(), ","))
	}

	if cfg == nil {
		return journal, nil
	}

	err = journal.Configure(cfg)
	if err != nil {
		return nil, err
	}

	return journal, nil
}

func JournalTypes() []string {
	var ctypes []string
	for ctype, _ := range journalTypes {
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
