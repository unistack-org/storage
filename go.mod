module github.com/unistack-org/storage

go 1.26.0

require (
	github.com/OneOfOne/xxhash v1.2.8
	github.com/VictoriaMetrics/metrics v1.40.2
	github.com/google/uuid v1.6.0
	github.com/hashicorp/golang-lru v1.0.2
	go.etcd.io/bbolt v1.5.0
	go.etcd.io/raft/v3 v3.7.0
	go.unistack.org/micro-meter-victoriametrics/v5 v5.0.2
	go.unistack.org/micro/v5 v5.0.4
	golang.org/x/sys v0.50.0
	google.golang.org/protobuf v1.36.12
	sigs.k8s.io/yaml v1.6.0
)

require (
	github.com/kr/pretty v0.3.1 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
	github.com/valyala/fastrand v1.1.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
)

replace github.com/VictoriaMetrics/metrics => github.com/NickPak/metrics v1.43.1-perf
