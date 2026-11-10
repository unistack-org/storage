package crushmap

import (
	"strings"
	"testing"
)

// goodMap — мини-карта: 2 хоста, ssd+spool на каждом, правила ssd/hdd.
func goodMap() *Map {
	m := NewMap()
	m.Types = []*Type{{ID: 0, Name: "device"}, {ID: 1, Name: "host"}, {ID: 2, Name: "root"}}
	m.Devices = []*Device{
		{ID: 0, Name: "osd0", Class: "ssd"},
		{ID: 1, Name: "osd1", Class: "ssd"},
		{ID: 2, Name: "osd2", Class: "hdd"},
		{ID: 3, Name: "osd3", Class: "hdd"},
	}
	m.Buckets = []*Bucket{
		{Name: "cn01", TypeID: 1, TypeName: "host", ID: -1, Alg: "straw2", Hash: "rjenkins1",
			Items: []*Item{{ID: 0, Name: "osd0", Weight: 1, Pos: 0}, {ID: 2, Name: "osd2", Weight: 1, Pos: 1}}},
		{Name: "cn02", TypeID: 1, TypeName: "host", ID: -2, Alg: "straw2", Hash: "rjenkins1",
			Items: []*Item{{ID: 1, Name: "osd1", Weight: 1, Pos: 0}, {ID: 3, Name: "osd3", Weight: 1, Pos: 1}}},
		{Name: "root", TypeID: 2, TypeName: "root", ID: -3, Alg: "straw2", Hash: "rjenkins1",
			Items: []*Item{{ID: -1, Name: "cn01", Weight: 2, Pos: 0}, {ID: -2, Name: "cn02", Weight: 2, Pos: 1}}},
	}
	m.Rules = []*Rule{
		{Name: "rule-data-ssd", ID: 1, Type: 1, MinSize: 2, MaxSize: 3, Steps: []*Step{
			{Op: "take", Item: -3, ItemName: "root", ItemClass: "ssd"},
			{Op: "chooseleaf_firstn", Num: 0, ItemType: "host"},
			{Op: "emit"},
		}},
		{Name: "rule-data-hdd", ID: 2, Type: 1, MinSize: 2, MaxSize: 2, Steps: []*Step{
			{Op: "take", Item: -3, ItemName: "root", ItemClass: "hdd"},
			{Op: "chooseleaf_firstn", Num: 0, ItemType: "host"},
			{Op: "emit"},
		}},
	}
	return m
}

func TestCheckGoodMap(t *testing.T) {
	res := goodMap().Check()
	if !res.OK() {
		t.Fatalf("expected clean map, got: %v", res.Errors)
	}
	if len(res.Warnings) != 0 {
		t.Fatalf("expected no warnings, got: %v", res.Warnings)
	}
}

func TestCheckBadPos(t *testing.T) {
	m := goodMap()
	// item pos 3 при 2 детях: классика из Ceph-описаний
	m.Buckets[0].Items = append(m.Buckets[0].Items, &Item{ID: 3, Name: "osd3", Weight: 1, Pos: 3})
	res := m.Check()
	if res.OK() {
		t.Fatal("expected error for pos >= size")
	}
	if !strings.Contains(res.Errors[0], "has pos 3 >= size 3") {
		t.Fatalf("unexpected message: %v", res.Errors)
	}
}

func TestCheckRuleItemNotDefined(t *testing.T) {
	m := goodMap()
	m.Rules[0].Steps[0].ItemName = "rot" // опечатка в имени take
	res := m.Check()
	if res.OK() {
		t.Fatal("expected error for undefined take item")
	}
	joined := strings.Join(res.Errors, "\n")
	if !strings.Contains(joined, `in rule "rule-data-ssd" item "rot" not defined`) {
		t.Fatalf("unexpected errors: %v", res.Errors)
	}
	if !strings.Contains(joined, "take rot class ssd") {
		t.Fatalf("expected class suffix in message: %v", res.Errors)
	}
}

func TestCheckClassSemantics(t *testing.T) {
	m := goodMap()
	// ssd: 2 устройства, max_size 3 -> warning о нехватке
	res := m.CheckClass("ssd")
	if !res.OK() {
		t.Fatalf("unexpected errors: %v", res.Errors)
	}
	if len(res.Warnings) == 0 {
		t.Fatal("expected warning: 2 ssd devices < max_size 3")
	}
	// hdd: 2 устройства, max_size 2 -> чисто
	if res := m.CheckClass("hdd"); !res.OK() || len(res.Warnings) != 0 {
		t.Fatalf("expected clean hdd, got E=%v W=%v", res.Errors, res.Warnings)
	}
	// класс, которого нет в карте
	m.Rules[0].Steps[0].ItemClass = "nvme"
	res = m.Check()
	if res.OK() {
		t.Fatal("expected error for unknown class")
	}
	if !strings.Contains(strings.Join(res.Errors, "\n"), `class "nvme"`) {
		t.Fatalf("unexpected errors: %v", res.Errors)
	}
}

func TestCheckDomains(t *testing.T) {
	m := goodMap()
	// убираем osd1 (ssd на cn02) -> для ssd-правила остаётся 1 хост-домен,
	// chooseleaf firstn (0 => max_size 3) требует 3 — предупреждение про degraded.
	m.Devices = m.Devices[:1]
	m.Buckets[1].Items = []*Item{{ID: 3, Name: "osd3", Weight: 1, Pos: 1}}
	res := m.CheckClass("ssd")
	joined := strings.Join(res.Warnings, "\n")
	if !strings.Contains(joined, "доменов host") {
		t.Fatalf("expected domain shortage warning, got E=%v W=%v", res.Errors, res.Warnings)
	}
}

func TestCheckWeightsReweight(t *testing.T) {
	m := goodMap()
	// reweight osd1 -> 0 (drain): ssd-размещение проседает до 1 устройства
	res := m.CheckWeights("ssd", map[int32]float64{1: 0})
	if len(res.Warnings) == 0 {
		t.Fatal("expected warnings after draining osd1")
	}
	// reweight всех hdd -> 0: hdd-правило мёртво
	res = m.CheckWeights("hdd", map[int32]float64{2: 0, 3: 0})
	joined := strings.Join(res.Warnings, "\n") + strings.Join(res.Errors, "\n")
	if !strings.Contains(joined, "rule-data-hdd") {
		t.Fatalf("expected hdd rule flagged, got E=%v W=%v", res.Errors, res.Warnings)
	}
}

func TestCheckStructureErrors(t *testing.T) {
	m := goodMap()
	m.Buckets[0].Items = append(m.Buckets[0].Items, &Item{ID: 0, Name: "osd0", Weight: 1, Pos: 2})   // дубль osd0
	m.Buckets[2].Items = append(m.Buckets[2].Items, &Item{ID: -9, Name: "ghost", Weight: 1, Pos: 2}) // нет бакета
	m.Rules[1].MinSize = 3
	m.Rules[1].MaxSize = 2
	res := m.Check()
	joined := strings.Join(res.Errors, "\n")
	for _, want := range []string{"дублируется ребёнок", "отсутствует в buckets", "min_size 3 > max_size 2"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %q in: %v", want, res.Errors)
		}
	}
}
