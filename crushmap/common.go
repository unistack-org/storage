package crushmap

import (
	"fmt"
	"sort"
	"strconv"
)

const (
	ReplicatedPG = 1
	ErasurePG    = 3
)

type CrushBucketType uint16
type CrushAlgType uint8
type CrushBucketHashType uint8

const (
	CrushAlgInvalid CrushAlgType = iota
	CrushAlgUniform
	CrushAlgList
	CrushAlgTree
	CrushAlgStraw
	CrushAlgStraw2
)

const (
	CrushLegacyAllowedBucketAlgs = (1 << CrushAlgUniform) | (1 << CrushAlgList) | (1 << CrushAlgStraw)
)

var (
	crushAlgTypeStringMap = map[CrushAlgType]string{
		CrushAlgUniform: "uniform",
		CrushAlgList:    "list",
		CrushAlgTree:    "tree",
		CrushAlgStraw:   "straw",
		CrushAlgStraw2:  "straw2",
	}
	crushAlgStringTypeMap = map[string]CrushAlgType{
		"uniform": CrushAlgUniform,
		"list":    CrushAlgList,
		"tree":    CrushAlgTree,
		"straw":   CrushAlgStraw,
		"straw2":  CrushAlgStraw2,
	}
)

func (t CrushBucketType) String() string {
	return fmt.Sprintf(strconv.Itoa(int(t)))
}

func (t CrushBucketHashType) String() string {
	if t == 0 {
		return "rjenkins1"
	}
	return fmt.Sprintf(strconv.Itoa(int(t)))
}

func (t CrushAlgType) String() string {
	alg, ok := crushAlgTypeStringMap[t]
	if !ok {
		alg = "invalid"
	}
	return alg
}

func CrushAlgFromType(t CrushAlgType) (string, error) {
	alg, ok := crushAlgTypeStringMap[t]
	if !ok {
		return "", fmt.Errorf("unknown crush bucket alg: %d", t)
	}
	return alg, nil
}

func CrushAlgFromString(t string) (CrushAlgType, error) {
	alg, ok := crushAlgStringTypeMap[t]
	if !ok {
		return CrushAlgInvalid, fmt.Errorf("unknown crush bucket algo: %s", t)
	}
	return alg, nil
}

type Tunables struct {
	// new block
	ChooseLocalTries         uint32 `json:"choose_local_tries,omitempty"`
	ChooseLocalFallbackTries uint32 `json:"choose_local_fallback_tries,omitempty"`
	ChooseTotalTries         uint32 `json:"choose_total_tries,omitempty"`
	// new block must be equal 1
	ChooseleafDescendOnce uint32 `json:"chooseleaf_descend_once,omitempty"`
	// new block must be equal 1
	ChooseleafVaryR uint8 `json:"chooseleaf_vary_r,omitempty"`
	// new block must be equal 1
	StrawCalcVersion uint8 `json:"straw_calc_version,omitempty"`
	// new block must be equal ??
	AllowedBucketAlgs uint32 `json:"allowed_bucket_algs,omitempty"`
	// new block must be equal 1
	ChooseleafStable uint8 `json:"chooseleaf_stable,omitempty"`
	//

	/*
	   "profile": "firefly",
	   "optimal_tunables": 0,
	   "legacy_tunables": 0,
	   "minimum_required_version": "firefly",
	   "require_feature_tunables": 1,
	   "require_feature_tunables2": 1,
	   "has_v2_rules": 1,
	   "require_feature_tunables3": 1,
	   "has_v3_rules": 0,
	   "has_v4_buckets": 0,
	   "require_feature_tunables5": 0,
	   "has_v5_rules": 0
	*/
}

type Step struct {
	Op         string `json:"op"`
	Item       int    `json:"item,omitempty"`
	ItemName   string `json:"item_name,omitempty"`
	ItemClass  string `json:"item_class,omitempty"`
	Num        int32  `json:"num,omitempty"`
	ItemType   string `json:"type,omitempty"`
	ItemTypeID int32  `json:"-"`
}

type Rule struct {
	Name    string  `json:"rule_name"`
	ID      uint8   `json:"rule_id"`
	Ruleset uint8   `json:"ruleset,omitempty"`
	Type    uint8   `json:"type"`
	MinSize uint8   `json:"min_size,omitempty"`
	MaxSize uint8   `json:"max_size,omitempty"`
	Steps   []*Step `json:"steps"`
}

type Item struct {
	ID     int32   `json:"id"`
	Name   string  `json:"-,omitempty"`
	Weight float32 `json:"weight"`
	Pos    int     `json:"pos"`
}

type Bucket struct {
	Name     string          `json:"name"`
	TypeID   CrushBucketType `json:"type_id"`
	TypeName string          `json:"type_name"`
	Weight   float32         `json:"weight"`
	ID       int32           `json:"id"`
	IDClass  string          `json:"id_class,omitempty"`
	Alg      string          `json:"alg"`
	Hash     string          `json:"hash"`
	Size     uint32          `json:"-"`
	Items    []*Item         `json:"items"`
}

type Device struct {
	ID    int32  `json:"id"`
	Name  string `json:"name"`
	Class string `json:"class,omitempty"`
}

type Type struct {
	ID   int32  `json:"type_id"`
	Name string `json:"name"`
}

type ChooseArg struct {
	BucketID  int32     `json:"bucket_id,omitempty"`
	WeightSet []float64 `json:"weight_set,omitempty"`
	IDs       []int     `json:"ids,omitempty"`
}

type Map struct {
	Tunables   map[string]interface{} `json:"tunables,omitempty"`
	Devices    []*Device              `json:"devices"`
	Types      []*Type                `json:"types"`
	Buckets    []*Bucket              `json:"buckets"`
	Rules      []*Rule                `json:"rules"`
	ChooseArgs map[string]ChooseArg   `json:"choose_args,omitempty"`
}

type CrushChild struct {
	ID     int     `json:"id"`
	Name   string  `json:"name"`
	Weight float32 `json:"weight"`
}

type CrushTree struct {
	Type     string        `json:"type"`
	Name     string        `json:"name"`
	ID       int           `json:"id"`
	Children []*CrushChild `json:"children"`
}

type CrushRule struct {
	Data [][]string `json:"data"`
}

type Crushmap struct {
	Trees []*CrushTree `json:"trees"`
	Rules []*CrushRule `json:"rules"`
}

func (m *Map) rulesSort() {
	sort.Slice(m.Rules, func(i, j int) bool { return m.Rules[i].ID < m.Rules[j].ID })
}

func (m *Map) bucketsSort() {
	sort.Slice(m.Buckets, func(i, j int) bool { return m.Buckets[i].TypeID > m.Buckets[j].TypeID })
}

func (m *Map) GetTypeIDByName(name string) uint16 {
	for _, t := range m.Types {
		if t.Name == name {
			return uint16(t.ID)
		}
	}
	return 0
}

func (m *Map) GetBucketByName(name string) *Bucket {
	for _, b := range m.Buckets {
		if b.Name == name {
			return b
		}
	}
	return nil
}

func (m *Map) GetBucketByID(id int32) *Bucket {
	for _, b := range m.Buckets {
		if b.ID == id {
			return b
		}
	}
	return nil
}

func NewMap() *Map {
	return &Map{Tunables: make(map[string]interface{})}
}
