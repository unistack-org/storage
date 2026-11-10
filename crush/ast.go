package crush

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

const (
	CrushAlgInvalid CrushAlgType = iota
	CrushAlgUniform
	CrushAlgList
	CrushAlgTree
	CrushAlgStraw
	CrushAlgStraw2
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
	return strconv.Itoa(int(t))
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

type StepDecl struct {
	Op         string `json:"op"`
	Item       int    `json:"item,omitempty"`
	ItemName   string `json:"item_name,omitempty"`
	ItemClass  string `json:"item_class,omitempty"`
	Num        int32  `json:"num,omitempty"`
	ItemType   string `json:"type,omitempty"`
	ItemTypeID int32  `json:"-"`
}

type RuleDecl struct {
	Name    string      `json:"rule_name"`
	ID      uint8       `json:"rule_id"`
	Ruleset uint8       `json:"ruleset,omitempty"`
	Type    uint8       `json:"type"`
	MinSize uint8       `json:"min_size,omitempty"`
	MaxSize uint8       `json:"max_size,omitempty"`
	Steps   []*StepDecl `json:"steps"`
}

type Item struct {
	ID     int32   `json:"id"`
	Name   string  `json:"-,omitempty"`
	Weight float32 `json:"weight"`
	Pos    int     `json:"pos"`
}

type BucketDecl struct {
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

type Wrapper struct {
	Tunables   map[string]interface{} `json:"tunables,omitempty"`
	Devices    []*Device              `json:"devices"`
	Types      []*Type                `json:"types"`
	Buckets    []*BucketDecl          `json:"buckets"`
	Rules      []*RuleDecl            `json:"rules"`
	ChooseArgs map[string]ChooseArg   `json:"choose_args,omitempty"`
}

func (m *Wrapper) rulesSort() {
	sort.Slice(m.Rules, func(i, j int) bool { return m.Rules[i].ID < m.Rules[j].ID })
}

func (m *Wrapper) bucketsSort() {
	sort.Slice(m.Buckets, func(i, j int) bool { return m.Buckets[i].TypeID > m.Buckets[j].TypeID })
}

func (m *Wrapper) GetTypeIDByName(name string) uint16 {
	for _, t := range m.Types {
		if t.Name == name {
			return uint16(t.ID)
		}
	}
	return 0
}

func (m *Wrapper) GetBucketByName(name string) *BucketDecl {
	for _, b := range m.Buckets {
		if b.Name == name {
			return b
		}
	}
	return nil
}

func (m *Wrapper) GetBucketByID(id int32) *BucketDecl {
	for _, b := range m.Buckets {
		if b.ID == id {
			return b
		}
	}
	return nil
}

func NewWrapper() *Wrapper {
	return &Wrapper{Tunables: make(map[string]interface{})}
}
