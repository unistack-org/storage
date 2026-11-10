module github.com/unistack-org/sds-nbd

go 1.26.0

require (
	github.com/google/uuid v1.6.0
	github.com/prometheus/client_golang v1.25.0
	github.com/prometheus/client_model v0.6.3
	github.com/unistack-org/storage v0.0.0
	sigs.k8s.io/yaml v1.4.0
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/common v0.72.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	github.com/unistack-org/crushkv v0.0.0-20261107123532-3e0600f56452 // indirect
	go.etcd.io/bbolt v1.5.0 // indirect
	go.etcd.io/raft/v3 v3.7.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
)

replace github.com/unistack-org/storage => ../
