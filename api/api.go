package api

/*
import (
	"fmt"
	"strings"

	"github.com/sdstack/storage/kv"
)

var apiTypes map[string]Api

func init() {
	apiTypes = make(map[string]Api)
}

func RegisterApi(engine string, api Api) {
	apiTypes[engine] = proxy
}

// Proxy represents proxy interface
type Proxy interface {
	Start() error
	Stop() error
	Configure(*kv.KV, interface{}) error
	//	Format() error
	//	Check() error
	//	Recover() error
	//	Info() (*Info, error)
	//	Snapshot() error
	//	Reweight() error
	//	Members() []Member
}

func New(ptype string, cfg interface{}, engine *kv.KV) (Proxy, error) {
	var err error

	proxy, ok := proxyTypes[ptype]
	if !ok {
		return nil, fmt.Errorf("unknown proxy type %s, only %s supported", ptype, strings.Join(ProxyTypes(), ","))
	}

	err = proxy.Configure(engine, cfg)
	if err != nil {
		return nil, err
	}

	return proxy, nil
}

func ProxyTypes() []string {
	var ptypes []string
	for ptype, _ := range proxyTypes {
		ptypes = append(ptypes, ptype)
	}
	return ptypes
}
*/
