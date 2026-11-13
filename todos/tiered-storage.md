# Tiered Storage: HDD + SSD — план и спеки

> Дата: 2026-11-13. Источник: исходники Vitastor 3.2.3 (`ccfacd1c`, зеркало
> `github.com/vitalif/vitastor`, канон — `git.yourcmc.ru`) + наша кодовая база
> (состояние на 2026-11-13) + Linux `bcache` (docs/разбор).
> Связано: `vitastor-ideas.md` §2.6/§8 (bcache-кэш), `roadmap.md` Фаза 5/6,
> `plan-ec.md`, `bench-stand.md`, `adr-meta-index.md`.

Это **план работ + спеки** по каждому шагу, в отличие от `vitastor-ideas.md`
(сырой список идей). Порядок шагов: от дешёвого (подъём существующего WAL на
отдельное устройство) к дорогому (read-кэш, placement-tiering). Каждый шаг
самодостаточен и даёт измеримый эффект отдельно от остальных.

---

## 0. Что уже есть (не переписывать)

| Механизм | Где | Роль в tiering |
|---|---|---|
| WAL journal + background applier + replay | `pkg/store/journal.go` | готовый **writeback-тир**: ack на fsync журнала, аплайер «распрямляет» записи в объекты. Но журнал лежит в каталоге объектов → одно устройство |
| `journalMaxOverlay=8192`, `journalApplyBatch=4096`, `journalApplyTick=20ms` | `pkg/store/journal.go:66-80` | бэкопёрессинг: журнал = ring, не «наполняемый магазин» |
| `JournalStats()` (logBytes, pending, free, total) | `pkg/store/journal.go:691`, `pkg/osd/osd.go:454` | метрики staging-футпринта журнала — уже на месте, покажут занятость SSD-партиции |
| in-RAM meta-индекс `objMeta` (24 Б/объект) | `pkg/store/meta.go` | источник истины для existence/size без `os.Stat`; дешевле bcache BSET |
| atomic writes (RWF_ATOMIC/AWUPF) + Write Intent Journal | `pkg/store/atomic.go`, `pkg/store/intent.go` | crash-safety write-пути; заменяют bcache BEP (см. §1.2) |
| commit modes none/flush/sync/journal | `pkg/store/store.go`, `pkg/osd/osd.go` | `-commit journal` = тот самый SSD-journal→HDD-objects writeback |
| volume-class (SSD/HDD) validator | `crush/check.go` (`CheckClass`, `CheckWeights`, `domainsForClass`, `ruleClass`) | готовый механизм placement-тира на уровне CRUSH (Шаг 2) |
| local read preference | `Volume.orderReadRefs` (`pkg/client`) | «локальный → наименее загруженный → pg-строка» — базис для read-tiering |
| 0-alloc data path (NBD, sdsnet, client Volume) | гейты в CI | кэш-слой обязан вписаться в 0 allocs/op, иначе деградирует |
| VDUSE readahead (пер-очередной sequential-детектор + окно) | `pkg/vduse/readahead.go` | аналог generation-token для invalidation уже есть (см. Шаг 1) |

Ключевой рычаг: **журнал и объекты сегодня на одном устройстве**
(`pkg/store/journal.go:66` `journalFileName = "journal.log"`, `openJournal(dir)`
→ `path.Join(dir, journalFileName)`, `dir` = каталог объектов). Развести их по
устройствам = весь writeback-тир уже работает — нужно только дать журналу
отдельный каталог/устройство.

---

## 1. Разбор: как у Vitastor и что у bcache

### 1.1 Vitastor: реализованного tiering НЕТ

В v3.2.3 tiering — только roadmap-пункт + два слайда презентации HL'2025:

- `docs/intro/features.{en,ru}.md:72` — «Tiered storage (SSD caching)»
  (future).
- Презентация: «Распределённый SSD-кэш (tiering)» и «Локальный SSD-кэш
  (bcache)» — обе future, без дизайн-дока, политики и таймлайна.

Ближайшее к гибриду в **коде** Vitastor:

1. **3-устройственная раскладка OSD** (data / meta / journal). Журнал —
   «must be on a fast SSD», ~1 ГБ для гибридных стендов. Это log-structured
   journaling, **не** кэш. У нас аналог уже есть (наш `commit journal`), только
   мы храним журнал рядом с данными (2 устройства: data+journal), а Vitastor —
   3 (data+meta+journal), т.к. у него meta на диске, а у нас — в RAM.
2. **`local_reads`** — политика чтения primary/nearest/random. Это
   **размещение**, не кэширование.
3. **`client_enable_writeback`** (`cluster_client_wb.cpp`) — in-RAM
   dirty-buffer coalescing на клиенте. Производительность, не disk-кэш.
4. **`meta_format=3`** (3.0.0, `a67c415e`) — log-structured store, WA≈1.
   Это про мету, не про data-tiering.

**Вывод:** у Vitastor нет ни read-кэша, ни writeback-кэша на SSD в коде. Их
«tiering» = то, что мы называем «журнал на SSD» (Шаг 0) + обещанный, но
не написанный local SSD-кэш. Наш Шаг 0 — ровно их 3-й диск (journal) +
разведение по устройствам, а Шаги 1–2 — то, чего у них нет в коде.

### 1.2 bcache: что переносимо, что — нет

`bcache` (ядро) = dm-кэш: SSD-кэш (bdev cache) перед HDD backing-кэш (bdev
backing), режимы writeback / writethrough / writearound / none + BSET
(разметка блоков в кэше) + BEP (block entry points, crash-recovery кэша).

**Главный переносимый принцип:** backing (HDD, реплицированный = source of
truth) + SSD-кэш = **disposable accelerator**. Кэш можно в любой момент
удалить и перестроить — данные не потеряны. Это снимает с read/write-through
путей обязанность «кэш = точка ack» и, как следствие, убирает из read/write-
through-кэша всё, что bcache делает для writeback-безопасности (BSET/BEP,
суперблок кэша, crash-recovery кэша).

| Что в bcache | Переносимо в UniStack? | Комментарий |
|---|---|---|
| SSD = disposable, HDD = truth | ✅ да | уже наша модель (реплицированные объекты) |
| read-through кэш + invalidation | ✅ да (ядро кэша) | `pkg/store` не знает, что данные реплицированы — кэш живёт на OSD-пути; invalidation по (oid, range) см. ниже |
| BSET (разметка блоков кэша) | ⚠️ частично | наша in-RAM meta дешевле; но нужен свой словарь (key → offset) |
| BEP / crash-recovery кэша | ❌ не нужен для read/write-through | у нас нет «кэш = ack» → кэш не персистентно-значимый |
| writeback-режим (кэш = точка ack) | ❌ сознательно откладываем | самый опасный режим: кэш становится durable-точкой (см. §4) |
| LRU / clock + bypass-порог | ✅ да | классика, портится в read-кэш |
| sequential-detection / readahead | ✅ да | у нас уже есть VDUSE readahead — переиспользуем детектор |
| dm-устройство (in-kernel) | ❌ не берём | это ядро; мы userspace-Go, кэш в OSD-пути |

**Generation-token для invalidation:** у нас уже есть аналог паттерна в
VDUSE readahead (per-queue `token`, `pkg/vduse/readahead.go:76`). Для read-кэша
в read-кэшах нужен глобальный `cacheGen` (atomic u64), который инкрементируется
при каждом ack-нутом write (`journalWrite`) и Remove; слот кэша хранит
`gen` на момент захвата, read сравнивает. Это даёт точную инвалидацию без
полного сброса кэша.

---

## 2. Целевая раскладка: 4 SSD + 4 HDD (1:1)

### 2.1 Почему 1:1, а не общий stripe из 4 SSD

Журнал **не** disposable (в отличие от read-кэша). Под `commit journal` ack
клиента идёт на fsync журнала ⇒ журнал держит хвост «ack-нуто, но ещё не на
HDD». Потеря журнала = потеря свежих записей **того** OSD (кластер живёт за
счёт реплик + обычной recovery, но это не «пересобери кэш»).

| Схема | Blast radius при смерти 1 SSD |
|---|---|
| 4 SSD → 1 stripe, все 4 журнала внутри | 4 журнала гибнут сразу = 4 OSD degraded одновременно |
| 1:1 (каждый HDD-OSD → свой SSD) | ровно 1 OSD degraded, rep3/min_size держит |

1:1 совпадает с существующей моделью «диск = OSD» (failure domain) и
device-pinning (`-data-dir uuid=X@dir`, `devices.json`). Общий stripe ломает
и то, и другое.

### 2.2 Размеринг журнала (это ring, не магазин)

`applier truncates` журнал (`journalApplyBatch=4096`), `journalMaxOverlay=8192`
— бэкопёрессинг не даёт журналу разрастаться бесконечно. Поэтому достаточно
маленькой SSD-партиции. Формула:

```
journal_min ≈ peak_iops_osd × block × (fsync_period + applier_lag) × 2
```

Практически: **10–50 ГБ на OSD**. Vitastor рекомендует ~1 ГБ — мы берём ×10
запас (и буфер на «медленный HDD при бёрсте»). `JournalStats().logBytes`
покажет реальный пик, размер можно сузить по замеру.

### 2.3 Конкретный layout 4+4

```
HDD1  /data/osd1   xfs          # объекты
HDD2  /data/osd2   xfs
HDD3  /data/osd3   xfs
HDD4  /data/osd4   xfs

SSD1  p1 /journal/osd1   20G    xfs noatime   # журнал (ring)
SSD1  p2 /cache/osd1     rest  xfs noatime   # read-кэш (Шаг 1)
SSD2  p1 /journal/osd2   20G    ...
SSD2  p2 /cache/osd2     rest
SSD3  p1 /journal/osd3   20G    ...
SSD3  p2 /cache/osd3     rest
SSD4  p1 /journal/osd4   20G    ...
SSD4  p2 /cache/osd4     rest

unistord ... -data-dir /data/osd1 -journal-dir /journal/osd1 -commit journal
             -data-dir /data/osd2 -journal-dir /journal/osd2 -commit journal
             -data-dir /data/osd3 -journal-dir /journal/osd3 -commit journal
             -data-dir /data/osd4 -journal-dir /journal/osd4 -commit journal
```

Варианты:

- **write-heavy**: журнал 20 ГБ + остаток под кэш (выше).
- **read-heavy**: журнал 5–10 ГБ + остаток под кэш.
- **2:1** (два журнала на один SSD, 2 свободных SSD под общий кэш) — только
  если нужно; 1:1 чище на старте.

### 2.4 Детали, которые надо сделать сразу

- **NUMA/PCIe-соседство:** SSD N в том же PCIe-root/backplane, что и HDD N
  (иначе «SSD-ускоритель» отдаляется на cross-NUMA hop и теряет смысл).
- **Кавычку (cache-партицию) зарезать сразу** — репартиционировать потом
  нельзя (нужен wipe). Даже если Шаг 1 отложить, `/cache/osdN` — это
  зарезервированная, не используемая пока партиция.
- **Пиннинг журнала по device:** расширить схему `uuid=X@dir` до
  `-journal-dir uuid=Y@<dir>` (то же, что для data, но отдельная identity),
  ИЛИ записать journal-identity в `devices.json`. Без пиннинга перепутанные
  SSD = потеря журнала = потеря ack-хвоста (см. §2.1).
- **Никогда не смешивать журнал + чужие данные на одном SSD.**
- **Meta/raft (кворум из 3 нод) — не на OSD-дисках.** Наша раскладка =
  2 устройства/OSD (data+journal), в отличие от Vitastor (3: data+meta+
  journal). Мета у нас в RAM (+ raft-репликация), так что 3-й диск не нужен.

---

## 3. План работ (по шагам, от дешёвого к дорогому)

### Шаг 0 — `-journal-dir`: журнал на отдельное устройство  [ОЦЕНКА: M — 2–4 дня]

Цель: развести WAL по устройству от объектов. Один маленький срез, превращающий
существующий `commit journal` в реальный writeback-тир **SSD-журнал →
HDD-объекты** с уже существующими crash-семантиками (replay при старте,
applier, bэкопёрессинг). Никакой новой логики кэша — только «куда писать
журнал».

#### Спек 0.1 — `store.Options.JournalDir`

`pkg/store/store.go`:

```go
type Options struct {
    ...
    // JournalDir, when non-empty, puts the write-ahead log in this
    // directory (a separate device) instead of the object directory.
    // Only meaningful for CommitMode=CommitJournal. The object files
    // (and any intent log) stay in dir; only the WAL moves. Replay at
    // open reads JournalDir/journal.log. A non-empty JournalDir with a
    // non-journal commit mode is rejected at Open.
    JournalDir string
}
```

`pkg/store/journal.go`:

- `openJournal(dir, logf)` → `openJournal(journalDir, logf)`, где
  `journalDir` = `opts.JournalDir` if set else `dir`. Внутри:
  `p := path.Join(journalDir, journalFileName)` (заменить `dir`).
- `store.go:275` (`js, err := openJournal(dir, opts.Logf)`) → передать
  `effectiveJournalDir(opts)`.
- `store.go:698` (`os.Stat(path.Join(s.dir, journalFileName))` в
  `JournalStats`) → `effectiveJournalDir`.
- `store.go:1132` (readdir-скачок имени `journalFileName`/`intentFileName`
  в `Load`) — оставить как есть: journal **не** попадает в object-dir
  readdir (он на другом устройстве), но на случай «journal без JournalDir»
  скачок обязателен.
- Валидация в `New`: `JournalDir != "" && CommitMode != CommitJournal` →
  `error("store: JournalDir requires -commit journal")`.

`effectiveJournalDir`:

```go
func effectiveJournalDir(o Options) string {
    if o.CommitMode == CommitJournal && o.JournalDir != "" {
        return o.JournalDir
    }
    return o.dir // не используется напрямую, но сохраняет поведение
}
```

#### Спек 0.2 — plumbing через `pkg/osd` и `cmd/unistord`

`pkg/osd/osd.go`:

- `Options`: добавить `JournalDir string` рядом с `DataDir`.
- `Run`: `store.New(d.opts.DataDir, store.Options{ ..., JournalDir:
  d.opts.JournalDir })`.
- `os.MkdirAll(d.opts.JournalDir, 0o755)` при запуске (только если non-empty).

`cmd/unistord/osd_boot.go`:

- `dataDirSpec` → добавить `journalDir string` (и, опционально,
  `journalDev string` для пиннинга, см. 0.4).
- Парсинг: сейчас `-data-dir` и `-journal-dir` — отдельные повторяемые флаги
  (`main.go:192`). Связать их по **порядку** (i-й data-dir ↔ i-й
  journal-dir) ИЛИ, чистее, расширить grammar `-data-dir`:
  `uuid=X@<data-dir>` → добавить `:journal=uuid=Y@<journal-dir>` /
  `:journal=<journal-dir>`. **Рекомендую второй** — связь data↔journal
  становится self-describing в одном аргументе и не зависит от порядка.

`cmd/unistord/main.go`:

- Добавить флаг `-journal-dir` (повторяемый) ИЛИ только расширить
  `parseDataDirSpec` (рекомендация). В `bootOSD`/`assignDataDirIDs` передать
  `journalDir` дальше в `osd.Options.JournalDir`.

#### Спек 0.3 — пиннинг журнала по device identity

Расширить `devices.json` / `<dir>/device` на вторую identity:

- `dataDirSpec{ dir, wantDev, journalDir, wantJournalDev }`.
- `readOSDPins` читает `<journalDir>/device` (journal-identity) и
  `<dir>/device` (data-identity).
- `checkDataDirSwaps`: сверить live-identity журнала с зарегистрированной.
  Перепутанный SSD-журнал = фатальная ошибка на боте (не тихая потеря
  ack-хвоста).

Минимально жизнеспособно: если `-journal-dir` задан без identity, просто
warn-лог «journal not device-pinned» и дальше. Полная pinning — follow-up.

#### Спек 0.4 — тесты

- `pkg/store/journal_test.go`: новый тест — `JournalDir` задан, журнал
  появляется в `JournalDir/journal.log`, **не** в object-dir; replay на reopen
  с тем же `JournalDir` восстанавливает объекты; `JournalDir` +
  non-journal mode → ошибка.
- `cmd/unistord/osd_spec_test.go` / `boot_devices_test.go`: парсинг
  `:journal=`; order/identity связность; swap-детект журнала.
- 0-alloc гейт: новый путь не должен добавлять аллокаций в hot read/write
  (проверка `TestStoreZeroAllocGate`-семейства, если применимо к journal-
  записи).

#### Критерии готовности Шага 0

- [ ] `-data-dir ...:journal=...` (или отдельный `-journal-dir`) поднимает OSD
      с журналом на ином устройстве; объект-файлы на HDD, `journal.log` на SSD.
- [ ] Crash-тест: kill -9 посреди записи → reopen → данные восстановлены
      (replay читает журнал с SSD, аплайер распрямляет на HDD).
- [ ] `JournalStats()` отражает SSD-партицию (free/total = каталог журнала).
- [ ] Перепутанный SSD-журнал = отказ на боте (swap-детект).
- [ ] Без `JournalDir` поведение **точно** как сейчас (регрессия нет).
- [ ] Бенч: `sdsbench` 4K random-write на `commit journal` — сравнить до/после
      на hybrid-стенде (журнал SSD vs HDD). Ожидаем рост write-IOPS (fsync
      уходит с HDD на SSD).

---

### Шаг 1 — Read-through block-кэш на SSD  [ОЦЕНКА: L — 1–2 недели]

Цель: ядро bcache-кэша — read-through кэш блоков на SSD, invalidation по
(oid, range) + generation-token. Backing = HDD-объекты (истина), кэш
disposable.

#### Спек 1.1 — где живёт кэш и какая единица

Кэш **на уровне OSD (store)**, не на клиенте: store уже знает, что объект
4 МБ (`object.Size = 1<<22`), и все read идут через `Store.ReadAt(oid, p, off)`.
Клиент/сdsnet реплицируют — кэш репликации не видит и не должен.

Единица кэша: **блок 4 КиБ** (страница; то, что реально ходит по read-пути —
см. `BenchmarkSDSNetRead4K`). Ключ = `(oid, blockIndex)` где
`blockIndex = off / 4096`. Размер записи в кэше = 4 КиБ (или 64 КиБ = 16
блоков, если хотим уменьшить кол-во слотов; начать с 4 КиБ, параметр
`blockCacheSlot`).

Почему не 4 МБ (весь объект): кэш на SSD 1–300 ГБ не вместит «каждый объект
целиком» при рабочих объёмах; и sequential-доступу нужен не весь объект, а
окно. Блочный кэш = классика, как у bcache.

#### Спек 1.2 — структура данных

```go
// pkg/store/blockcache.go (новый)
type BlockCache struct {
    dir   string        // SSD-каталог (cache-партиция), напр. /cache/osdN
    cap   int64         // максимум байтов в кэше (параметр)
    slot  int           // размер слота: 4096 или 65536
    gen   atomic.Uint64 // глобальный cache-генерация

    // key = (oid, blockIndex) → слот
    // LRU/clock: использовать ring-структуру + пер-слот state
    // ...
}
```

- Персистентность: **опционально**. MVP = in-RAM index + data-on-SSD
  (перезагрузка = кэш пуст, rebuild по read). Полный bcache-стиль
  (per-слот header на SSD, crash-recovery кэша) — **не** делаем: кэш
  disposable, rebuild дёшев. Это главный выигрыш от «кэш ≠ точка ack».
- Eviction: clock/LRU + **bypass-порог** (если слот «не попал в кэш N раз
  подряд», помечить bypass → не кэшировать, как bcache sequential/
  non-repetitive bypass).

#### Спек 1.3 — инвалидация через generation-token

Ключевое, без чего кэш читал бы stale данные:

- `store.go`: при каждом ack-нутом мутации (в journal-режиме —
  `journalWrite`/`journalZeroes`/`journalRemove`; в non-journal —
  `WriteAt`/`WriteZeroes`/`Remove`) → `bcache.Invalidate(oid, off, n)`:
  (a) удалить затронутые слоты (b) `bcache.gen.Add(1)`.
- Слот хранит `gen` на момент записи. `ReadAt` берёт слот и сравнивает
  `slot.gen == bcache.gen.Load()`: не совпало → miss (слот stale, не
  обязательно удалён — generation даёт **быструю** проверку без точной
  инвалидзации каждого слота, как у bcache BSET-верификации, но дешевле).
- Точная инвалидзация (удаление по range) + generation — двойная защита:
  range-удаление ловит большинство, generation ловит промахи.

Это ровно тот паттерн, что уже в `pkg/vduse/readahead.go` (per-queue token)
— только глобальный и на уровне store.

#### Спек 1.4 — интеграция в read-path

`pkg/store/store.go` `ReadAt` (сейчас строка 556):

```go
func (s *Store) ReadAt(oid object.OID, p []byte, off int64) (int, error) {
    ...
    if s.jrn != nil {
        return s.journalReadAt(oid, p, off) // overlay поверх кэша? см. 1.5
    }
    if s.cache != nil {
        if n, hit := s.cache.Read(oid, p, off); hit {
            return n, nil
        }
    }
    e, err := s.open(oid, false)
    ...
    n, err := e.f.ReadAt(p, off) // backing (HDD)
    if err == nil && s.cache != nil {
        s.cache.Fill(oid, p, off, n) // fill на hit-backing (не в read-пути — async)
    }
    return n, err
}
```

Fill = **не блокирующий read-path**: засунуть в очередь/арену и заполнить
кэш в фоне (иначе read-кэш добавляет латентность). Для 0-alloc: слоты из
зарегистрированной арены (см. `RingArenaSlots`), не per-request malloc.

#### Спек 1.5 — взаимодействие с journal-overlay

`journalReadAt` (store.go:576) оверлеит staged-записи поверх файла. Кэш
должен кэшировать **уже оверлеенный** результат (итог `ReadAt`), а не
сырой файл. Проще: кэш в non-journal режиме на 1-м этапе, в journal-режиме
— кэшит результат `journalReadAt` (после оверлея). Так stale-окно
минимально: оверлей применяется, результат кэшируется, generation
инвалидирует при следующем write.

#### Спек 1.6 — флаги и параметры

`store.Options`:

```go
BlockCacheDir string // SSD-каталог кэша ("" = выкл)
BlockCacheBytes int64 // max кэша (0 = выкл/авто)
BlockCacheSlot  int   // размер слота (4096 default)
```

`pkg/osd.Options`: `BlockCacheDir`, `BlockCacheBytes`, `BlockCacheSlot`.
`cmd/unistord`: `-block-cache-dir`, `-block-cache-mb`, `-block-cache-slot`.

#### Спек 1.7 — тесты

- `pkg/store/blockcache_test.go`: hit/miss; write→invalidate→read даёт
  свежие данные (stale-тест); generation-тест (write без точного range —
  слот по-прежнему invalid по gen); evict при переполнении; bypass-
  последовательного потока (sequential не кэшируется).
- Crash: убийство с заполненным кэшем → reopen → кэш пуст, данные
  корректны (пересборка).
- 0-alloc гейт: read-path через кэш = 0 allocs/op (стенды `*ZeroAllocGate`).
- Конкуренция: `-race` на hit/evict/invalidate параллельно.

#### Критерии готовности Шага 1

- [ ] Random read IOPS на hybrid-стенде выше, чем без кэша (кэш SSD
      обслуживает hot-блоки). Цель: сближение с `bench-stand.md` 171–177K →
      выше.
- [ ] Stale-тест пройден: ни один read не вернёт данные старше последнего
      ack-нутого write.
- [ ] Кэш выключается одним флагом; без флага поведение как сейчас.
- [ ] Eviction + bypass не «сжигают» кэш на sequential (последовательный
      поток не должен выгонять hot-блоки).
- [ ] 0 allocs/op на read-пути через кэш.

---

### Шаг 2 — Placement-level read tiering (volume-class + local-reads)  [ОЦЕНКА: M — 3–5 дней]

Цель: не disk-кэш, а **размещение** — читать «с ближайшего/быстрого» OSD,
как `local_reads` у Vitastor. Это дёшево и даёт эффект для read-heavy без
кэша.

#### Спек 2.1 — что уже есть

- `crush/check.go`: `CheckClass`, `CheckWeights`, `domainsForClass`,
  `ruleClass` — SSD/HDD классы в CRUSH уже валидируются.
- `Volume.orderReadRefs` (`pkg/client`) — порядок предпочтений «локальный →
  наименее загруженный → pg-строка».
- `meta.OSDInfo` (`pkg/meta/records.go:33`) — уже несёт `Weight`, `Device`.
  Добавить `Class` (ssd|hdd|generic) → OSD-регистрация публикует класс.

#### Спек 2.2 — что добавить

- `OSDInfo.Class string` (json `class,omitempty`). OSD на старте знает свой
  класс (из `-osd-class` / из `devices.json` / из `-data-dir ...:class=ssd`)
  и публикует в мета-регистрации.
- `orderReadRefs`: приоритет реплик — «локальный → **быстрый класс
  (SSD)** → наименее загруженный → pg-строка». Флаг `-local-reads
  {off|class|nearest}` (по мотивам Vitastor) включает/выбирает политику.
- CRUSH rule: опциональный read-only variant-путь, чтобы placement мог
  предпочесть SSD-домен для **чтения** (без изменения write-placement).

#### Спек 2.3 — тесты

- `crush/check_test.go`: классы ssd/hdd проходят валидацию (уже есть) +
  ruleClass-разбор.
- `pkg/client`: `orderReadRefs` с `-local-reads class` ставит SSD-реплику
  выше HDD-реплики (при прочих равных).
- e2e: volume на `replicas=3` (2 HDD + 1 SSD) → чтение идёт с SSD-реплики.

#### Критерии готовности Шага 2

- [ ] OSD публикует `Class` в регистрации; `unistorctl` видит класс.
- [ ] `-local-reads class` переключает порядок чтения; без флага — как
      сейчас.
- [ ] Read-latency на hybrid-стенде улучшен на hot-объёмах (чтение с
      «быстрого» домена).

---

### Шаг 3 (отложен) — Writeback-кэш как durable-точка  [НЕ БРАТЬ]

bcache-режим writeback: SSD-кэш = точка ack, HDD = lazy. **Сознательно
не делаем**, причины:

1. Кэш становится durable-точкой ⇒ ему нужны BSET/BEP, суперблок,
   crash-recovery (всё то, что мы ушли из read/write-through-кэша).
2. У нас уже есть **настоящий** writeback — журнал (`commit journal`):
   ack на fsync журнала (SSD), аплайер распрямляет на HDD. Разница с
   writeback-кэшем: журнал = WAL (append-only, replay-able), кэш =
   read/modify-словарь. WAL безопаснее и уже есть.
3. Репликация: у Vitastor writeback «soft» — запись ack-нуется до кворума
   (vitastor-ideas §4.1). У нас кворум (min_size) — базовая гарантия;
   writeback-кэш, ломающий «ack = кворум», ломает и эту модель.

**Вывод:** writeback-тир уже есть (журнал). Отдельный writeback-кэш —
дублирование с худшей crash-семантикой. Не берём.

### Отложено сознательно (не в ближайших шагах)

- **Client-side data read-кэш** (кэш блоков на клиенте/в госте) — это уже
  гостевой FS-кэш + наш writeback; дублирует ОС-страницы. Не берём.
- **In-kernel dm-bcache** — это ядро, мы userspace-Go. Не берём.
- **Распределённый SSD-кэш** (Vitastor-«tiering», общий кэш на несколько
  OSD) — дорого, сложная инвалидация по кластеру. Локальный кэш (Шаг 1)
  закрывает 90% эффекта; распределённый — только когда локальный кэш
  «не хватает» по hit-rate.

---

## 4. Порядок и зависимости

```
Шаг 0 (-journal-dir)          [M, 2–4 дня]   ← начать здесь, самодостаточен
   │   превращает commit journal в SSD-журнал→HDD writeback
   ▼
Шаг 1 (read-through кэш)     [L, 1–2 нед]    ← зависит от 0 (кэш на той же SSD-партиции, что и журнал)
   │   read-tier на SSD
   ▼
Шаг 2 (local-reads/класc)    [M, 3–5 дней]   ← независим от 0/1, можно параллельно
```

Шаг 2 можно делать **параллельно** с 0/1 (не зависит от журнала и кэша —
только от placement/client). Шаги 0 → 1 последовательны (кэш и журнал
делят одну SSD-партицию: p1=journal, p2=cache — см. §2.3, партиционировать
надо сразу).

**Рекомендация:** Шаг 0 в ближайший спринт (малый срез, большой эффект на
write-heavy hybrid). Шаг 1 + 2 — следующий. Бенч-критерии каждого шага —
на hybrid-стенде (4 SSD + 4 HDD) через `sdsbench` + `bench-stand.md`
профили.

---

## 5. Что записать в roadmap.md (когда утвердят план)

- Фаза 5 (Производительность): добавить пункт «Tiered storage: Шаг 0
  `-journal-dir`» с чек-листом критериев.
- Фаза 6 (Функциональность): «Tiered: read-кэш (Шаг 1) + local-reads (Шаг 2)».
- Риски/решения: зафиксировать «writeback-кэш не берём (дублирует журнал)»
  и «кэш = disposable, не durable-точка (отсюда без BEP/BSET-crash-recovery)».
- Связки: `vitastor-ideas.md` §2.6/§8 (bcache-кэш) → pointer сюда;
  `plan-ec.md` — не затрагиваем (EC — отдельный трек).
