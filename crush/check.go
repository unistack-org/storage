package crush

// Валидатор crush-карты: компилируемость + семантика объёмных классов (SSD/HDD).
//
// Проверяет то, что молча ломает размещение, но не ловится компилятором:
//   - item 'slot1-3' in bucket 'cn01' has pos 3 >= size 3 (pos from 0 to len - 1 items)
//   - in rule 'rule-data-ssd' item 'root' not defined (check items in take action step take root class ssd)
//   - правило class ssd, под take нет ни одного ssd-устройства -> правило мёртвое
//   - chooseleaf firstn 3 type host, хостов с ssd два -> PG никогда не наберёт 3 реплики
//
// Check — структурная целостность карты (ломает компиляцию => Errors).
// CheckClass — семантика одного объёмного класса тома (SSD/HDD): правила,
// ёмкость доменов отказа, совместимость с репликами (риски => Warnings).
// CheckWeights — «что будет, если применить reweight»: считает размещение
// после замены весов, не меняя карту.

import (
	"fmt"
	"sort"
	"strings"
)

// CheckResult — вердикт проверки. Errors означают, что карта не скомпилируется
// или правило гарантированно ничего не размещает (exit != 0). Warnings —
// карта рабочая, но размещению не хватает ресурсов: часть PG уйдёт в
// degraded/undersized (например на время rebuild).
type CheckResult struct {
	Errors   []string
	Warnings []string
}

// OK — ошибок нет (warnings не учитываются).
func (r *CheckResult) OK() bool { return len(r.Errors) == 0 }

// AddError / AddWarning — для внешних валидаторов (placement и т.п.).
func (r *CheckResult) AddError(format string, args ...interface{}) {
	r.Errors = append(r.Errors, fmt.Sprintf(format, args...))
}

func (r *CheckResult) AddWarning(format string, args ...interface{}) {
	r.Warnings = append(r.Warnings, fmt.Sprintf(format, args...))
}

// Merge присоединяет другой вердикт.
func (r *CheckResult) Merge(o *CheckResult) {
	r.Errors = append(r.Errors, o.Errors...)
	r.Warnings = append(r.Warnings, o.Warnings...)
}

// Sort приводит списки к детерминированному порядку.
func (r *CheckResult) Sort() {
	sort.Strings(r.Errors)
	sort.Strings(r.Warnings)
}

// Check — структурная валидация карты: устройства, типы, бакеты, иерархия,
// правила и их шаги.
func (m *Wrapper) Check() *CheckResult {
	res := &CheckResult{}
	m.checkDevices(res)
	m.checkBuckets(res)
	m.checkRules(res)
	res.Sort()
	return res
}

func (m *Wrapper) checkDevices(res *CheckResult) {
	byID := map[int32]string{}
	byName := map[string]int32{}
	for _, d := range m.Devices {
		if d.ID < 0 {
			res.AddError("device %q: id %d должен быть неотрицательным", d.Name, d.ID)
		}
		if prev, dup := byID[d.ID]; dup {
			res.AddError("device %q: id %d уже занят %q", d.Name, d.ID, prev)
		}
		byID[d.ID] = d.Name
		if prev, dup := byName[d.Name]; dup {
			res.AddError("device %q: имя уже используется устройством %d", d.Name, prev)
		}
		byName[d.Name] = d.ID
	}
}

func (m *Wrapper) checkBuckets(res *CheckResult) {
	typeRank := map[string]int{}
	for i, t := range m.sortedTypes() {
		typeRank[t.Name] = i
	}

	seenID := map[int32]string{}
	inBucket := map[int32]int{}
	for _, b := range m.Buckets {
		if b.TypeName == "" {
			res.AddError("bucket %q: не указан тип", b.Name)
		} else if _, ok := typeRank[b.TypeName]; !ok {
			res.AddError("bucket %q: неизвестный тип %q (нет в секции types)", b.Name, b.TypeName)
		}
		if b.ID >= 0 {
			res.AddError("bucket %q: id %d должен быть отрицательным", b.Name, b.ID)
		}
		if prev, dup := seenID[b.ID]; dup {
			res.AddError("bucket %q: id %d уже занят %q", b.Name, b.ID, prev)
		}
		seenID[b.ID] = b.Name
		if !knownAlg(b.Alg) {
			res.AddError("bucket %q: неизвестный alg %q", b.Name, b.Alg)
		}

		seenItem := map[int32]string{}
		posSeen := map[int]string{}
		for _, it := range b.Items {
			if prev, dup := seenItem[it.ID]; dup {
				res.AddError("bucket %q: дублируется ребёнок %d (%s), уже упоминается как %s", b.Name, it.ID, it.Name, prev)
			}
			seenItem[it.ID] = it.Name
			// pos обязан попадать в диапазон индексов children: pos >= size
			// означает потерянный элемент при построении бакета.
			if it.Pos < 0 || it.Pos >= len(b.Items) {
				res.AddError("item %q in bucket %q has pos %d >= size %d (pos from 0 to len - 1 items)", it.Name, b.Name, it.Pos, len(b.Items))
			}
			if prev, dup := posSeen[it.Pos]; dup && it.Pos >= 0 {
				res.AddError("bucket %q: pos %d занят ребёнком %q", b.Name, it.Pos, prev)
			}
			posSeen[it.Pos] = it.Name
			if it.ID >= 0 {
				inBucket[it.ID]++
				if _, ok := m.DeviceByID(it.ID); !ok {
					res.AddError("bucket %q: устройство %d (%s) отсутствует в devices", b.Name, it.ID, it.Name)
				}
				continue
			}
			cb := m.GetBucketByID(it.ID)
			if cb == nil {
				res.AddError("bucket %q: дочерний бакет %d (%s) отсутствует в buckets", b.Name, it.ID, it.Name)
				continue
			}
			if typeRank[cb.TypeName] >= typeRank[b.TypeName] {
				res.AddError("bucket %q (%s): дочерний бакет %q имеет тип %q — он не ниже родительского",
					b.Name, b.TypeName, cb.Name, cb.TypeName)
			}
		}
	}
	// Устройства вне бакетов недоступны правилам; дубль в двух бакетах
	// делает размещение недетерминированным (двойной учёт веса).
	for _, d := range m.Devices {
		switch n := inBucket[d.ID]; {
		case n == 0:
			res.AddWarning("устройство %d (%s, class %s) не входит ни в один бакет — правила его не видят", d.ID, d.Name, d.Class)
		case n > 1:
			res.AddError("устройство %d (%s) встречается в %d бакетах — размещение будет недетерминированным", d.ID, d.Name, n)
		}
	}
}

func (m *Wrapper) checkRules(res *CheckResult) {
	knownType := map[string]bool{}
	for _, t := range m.Types {
		knownType[t.Name] = true
	}
	seenRuleID := map[uint8]string{}
	seenRuleName := map[string]uint8{}
	for _, ru := range m.Rules {
		if prev, dup := seenRuleID[ru.ID]; dup {
			res.AddError("rule %q: rule id %d уже занят %q", ru.Name, ru.ID, prev)
		}
		seenRuleID[ru.ID] = ru.Name
		if prev, dup := seenRuleName[ru.Name]; dup {
			res.AddError("rule %q: имя уже используется правилом id %d", ru.Name, prev)
		}
		seenRuleName[ru.Name] = ru.ID
		if ru.MinSize > ru.MaxSize {
			res.AddError("rule %q: min_size %d > max_size %d", ru.Name, ru.MinSize, ru.MaxSize)
		}
		if len(ru.Steps) == 0 {
			res.AddError("rule %q: пустой список шагов", ru.Name)
			continue
		}
		if last := ru.Steps[len(ru.Steps)-1]; last.Op != "emit" {
			res.AddError("rule %q: последний шаг должен быть emit, получено %q", ru.Name, last.Op)
		}
		for i, st := range ru.Steps {
			if st.ItemClass != "" && !m.classExists(st.ItemClass) {
				res.AddError("rule %q шаг %d: class %q не встречается ни в одном устройстве — шаг не выберет ничего", ru.Name, i, st.ItemClass)
			}
			switch st.Op {
			case "take":
				name := st.ItemName
				if name == "" {
					res.AddError("rule %q шаг %d: take %d — не найден бакет с таким id", ru.Name, i, st.Item)
					continue
				}
				if b := m.GetBucketByName(name); b != nil {
					continue
				}
				// компилятор допускает take по имени устройства
				if _, ok := m.deviceByName(name); ok {
					continue
				}
				res.AddError("in rule %q item %q not defined (check items in take action step take %s%s)",
					ru.Name, name, name, classSuffix(st.ItemClass))
			case "choose_firstn", "choose_indep", "chooseleaf_firstn", "chooseleaf_indep":
				if st.ItemType == "" {
					res.AddError("rule %q шаг %d: %s без type", ru.Name, i, st.Op)
				} else if !knownType[st.ItemType] {
					res.AddError("rule %q шаг %d: %s type %q — тип не объявлен в types", ru.Name, i, st.Op, st.ItemType)
				}
			case "choosemsr_firstn", "choosemsr_indep":
				res.AddError("rule %q шаг %d: msr-шаги не поддерживает движок этой сборки", ru.Name, i)
			case "":
				res.AddError("rule %q шаг %d: пустой шаг (не распознан)", ru.Name, i)
			default:
				if !knownSetOp(st.Op) && !knownChooseOp(st.Op) && st.Op != "emit" {
					res.AddError("rule %q шаг %d: неизвестный шаг %q", ru.Name, i, st.Op)
				}
			}
		}
	}
}

// knownSetOp — служебные шаги-модификаторы (compile.go их принимает).
func knownSetOp(op string) bool {
	switch op {
	case "set_choose_tries", "set_chooseleaf_tries", "set_choose_local_tries",
		"set_choose_local_fallback_tries", "set_chooseleaf_vary_r",
		"set_chooseleaf_stable", "set_msr_descents", "set_msr_collision_tries":
		return true
	}
	return false
}

func knownChooseOp(op string) bool {
	switch op {
	case "choose_firstn", "choose_indep", "chooseleaf_firstn", "chooseleaf_indep":
		return true
	}
	return false
}

func knownAlg(alg string) bool {
	switch alg {
	case "uniform", "list", "tree", "straw", "straw2":
		return true
	}
	return false
}

func classSuffix(class string) string {
	if class == "" {
		return ""
	}
	return " class " + class
}

// CheckClass — семантика объёмного класса тома (SSD/HDD) поверх карты.
// Отвечает на вопрос «запустятся ли тома этого класса по всем правилам»:
//   - правила с фильтром class == name должны находить устройства;
//   - под их take-корнем должно быть >= max_size устройств класса
//     (иначе часть PG будет inactive+undersized даже на полной карте);
//   - chooseleaf по типу T требует >= replicas доменов типа T с устройствами
//     класса — иначе правило не способно разложить реплики по отказоустойчивым
//     доменам (потеря одного хоста неизбежно роняет PG ниже min_size).
//
// Правила без фильтра класса считаются применимыми к любому классу и
// получают предупреждение: изоляции класса нет.
func (m *Wrapper) CheckClass(class string) *CheckResult {
	res := &CheckResult{}
	res.Merge(m.checkClassWeights(class, nil))
	res.Sort()
	return res
}

// CheckWeights — валидатор перевеса: применяет веса (CRUSH-единицы:
// 1.0 = вес 1.0, 0 = osd out) и отвечает, выживет ли размещение класса.
// weights == nil эквивалентно CheckClass.
func (m *Wrapper) CheckWeights(class string, weights map[int32]float64) *CheckResult {
	res := &CheckResult{}
	res.Merge(m.checkClassWeights(class, weights))
	res.Sort()
	return res
}

func (m *Wrapper) checkClassWeights(class string, weights map[int32]float64) *CheckResult {
	res := &CheckResult{}
	alive := func(id int32) bool {
		if weights == nil {
			return true
		}
		w, ok := weights[id]
		if !ok {
			return true // переопределённых устройств это не касается
		}
		return w > 0
	}
	for _, ru := range m.Rules {
		cls := ruleClass(ru)
		filtered := cls != ""
		if filtered && class != "" && cls != class {
			continue // чужой класс: тома class не размещаются этим правилом
		}
		if !filtered {
			cls = class // без фильтра правило ест любой класс -> считаем для этого
		}
		take := m.ruleTake(ru)
		devices := m.devicesForRule(cls, take, alive)
		label := "без фильтра класса"
		if filtered {
			label = "class " + cls
		}
		n := len(devices)
		if n == 0 {
			if filtered && cls == class {
				res.AddError("rule %q (%s): под take нет ни одного устройства класса %q — правило ничего не размещает", ru.Name, label, cls)
			} else {
				res.AddWarning("rule %q (%s): нет доступных устройств — правило сейчас ничего не размещает", ru.Name, label)
			}
			continue
		}
		maxSize := int(ru.MaxSize)
		if maxSize == 0 {
			maxSize = 100 // компилятор подставляет 100; ёмкость ограничена устройствами
		}
		minSize := int(ru.MinSize)
		if minSize == 0 {
			minSize = 1
		}
		// Домены отказа chooseleaf-ступени: distinct бакеты типа T с живыми
		// устройствами класса. num 0 означает «по числу реплик» (max_size).
		if leaf, num := ruleChooseLeaf(ru); leaf != "" {
			replicas := num
			if replicas <= 0 {
				replicas = maxSize // indep -1 = «сколько есть» — порог max_size консервативен
			}
			domains := m.domainsForClass(take, leaf, cls, alive)
			if domains < replicas {
				msg := fmt.Sprintf("rule %q (%s): chooseleaf type %s требует %d доменов %s, доступно %d — PG не соберут полные комплекты реплик",
					ru.Name, label, leaf, replicas, leaf, domains)
				if domains >= minSize && replicas > minSize {
					res.AddWarning("%s (min_size выдержим, режим degraded)", msg)
				} else {
					res.AddWarning("%s", msg)
				}
			}
		}
		if n < maxSize {
			res.AddWarning("rule %q (%s): доступно %d устройств (< max_size %d) — на полной карте часть PG будет inactive+undersized", ru.Name, label, n, maxSize)
		}
	}
	return res
}

// devicesForRule — устройства, до которых правило дотянется: под take-корнем,
// нужного класса, живые по весам.
func (m *Wrapper) devicesForRule(cls, take string, alive func(int32) bool) []*Device {
	set := map[int32]bool{}
	if take != "" {
		b := m.GetBucketByName(take)
		if b == nil {
			return nil
		}
		m.collectDevices(b, set)
	} else {
		for _, d := range m.Devices {
			set[d.ID] = true
		}
	}
	var out []*Device
	for _, d := range m.Devices {
		if !set[d.ID] {
			continue
		}
		if cls != "" && d.Class != cls {
			continue
		}
		if !alive(d.ID) {
			continue
		}
		out = append(out, d)
	}
	return out
}

// domainsForClass — число бакетов типа leafType (например host), под take
// которым есть хотя бы одно живое устройство класса.
func (m *Wrapper) domainsForClass(take, leafType, cls string, alive func(int32) bool) int {
	root := m.GetBucketByName(take)
	if root == nil {
		return 0
	}
	count := 0
	visited := map[int32]bool{}
	var walk func(b *BucketDecl)
	walk = func(b *BucketDecl) {
		if visited[b.ID] {
			return
		}
		visited[b.ID] = true
		if b.TypeName == leafType {
			if m.hasAliveDevice(b, cls, alive) {
				count++
			}
			return
		}
		for _, it := range b.Items {
			if it.ID < 0 {
				if cb := m.GetBucketByID(it.ID); cb != nil {
					walk(cb)
				}
			}
		}
	}
	walk(root)
	return count
}

func (m *Wrapper) hasAliveDevice(b *BucketDecl, cls string, alive func(int32) bool) bool {
	for _, it := range b.Items {
		if it.ID >= 0 {
			if d, ok := m.DeviceByID(it.ID); ok && (cls == "" || d.Class == cls) && alive(it.ID) {
				return true
			}
			continue
		}
		if cb := m.GetBucketByID(it.ID); cb != nil && m.hasAliveDevice(cb, cls, alive) {
			return true
		}
	}
	return false
}

// ruleClass — фильтр класса правила (первый step с item_class).
func ruleClass(ru *RuleDecl) string {
	for _, st := range ru.Steps {
		if st.ItemClass != "" {
			return st.ItemClass
		}
	}
	return ""
}

// ruleTake — имя take-корня правила ("" — take по id/устройству либо нет take).
func (m *Wrapper) ruleTake(ru *RuleDecl) string {
	for _, st := range ru.Steps {
		if st.Op == "take" {
			if st.ItemName != "" {
				return st.ItemName
			}
			if b := m.GetBucketByID(int32(st.Item)); b != nil {
				return b.Name
			}
			return ""
		}
	}
	return ""
}

// ruleChooseLeaf — тип домена первой chooseleaf-ступени и её num.
func ruleChooseLeaf(ru *RuleDecl) (string, int) {
	for _, st := range ru.Steps {
		if st.Op == "chooseleaf_firstn" || st.Op == "chooseleaf_indep" {
			return st.ItemType, int(st.Num)
		}
	}
	return "", 0
}

// Classes — отсортированный список классов устройств карты (для CLI-подсказок).
func (m *Wrapper) Classes() []string {
	set := map[string]bool{}
	for _, d := range m.Devices {
		if d.Class != "" {
			set[d.Class] = true
		}
	}
	out := make([]string, 0, len(set))
	for c := range set {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// classExists — встречается ли класс хотя бы у одного устройства.
func (m *Wrapper) classExists(class string) bool {
	for _, d := range m.Devices {
		if d.Class == class {
			return true
		}
	}
	return false
}

// DeviceByID — устройство по id (в Wrapper нет индекса, обход линейный).
func (m *Wrapper) DeviceByID(id int32) (*Device, bool) {
	for _, d := range m.Devices {
		if d.ID == id {
			return d, true
		}
	}
	return nil, false
}

func (m *Wrapper) deviceByName(name string) (*Device, bool) {
	for _, d := range m.Devices {
		if d.Name == name {
			return d, true
		}
	}
	return nil, false
}

// collectDevices — рекурсивно собирает id устройств под бакетом.
func (m *Wrapper) collectDevices(b *BucketDecl, into map[int32]bool) {
	seen := map[int32]bool{}
	var walk func(*BucketDecl)
	walk = func(x *BucketDecl) {
		if seen[x.ID] {
			return
		}
		seen[x.ID] = true
		for _, it := range x.Items {
			if it.ID >= 0 {
				into[it.ID] = true
			} else if cb := m.GetBucketByID(it.ID); cb != nil {
				walk(cb)
			}
		}
	}
	walk(b)
}

// sortedTypes — типы по возрастанию id (device -> ... -> root).
func (m *Wrapper) sortedTypes() []*Type {
	out := append([]*Type{}, m.Types...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// FormatCheckResult — человекочитаемый вывод `unistorctl map check`.
func FormatCheckResult(r *CheckResult) string {
	var sb strings.Builder
	for _, e := range r.Errors {
		sb.WriteString("ERROR: " + e + "\n")
	}
	for _, w := range r.Warnings {
		sb.WriteString("WARN:  " + w + "\n")
	}
	if r.OK() && len(r.Warnings) == 0 {
		sb.WriteString("ok\n")
	}
	return sb.String()
}
