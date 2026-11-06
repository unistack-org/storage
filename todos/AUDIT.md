# Аудит репозитория storage (срез 2026-11-06)

Репозиторий: https://github.com/unistack-org/storage (клон в `../storage`)
Всего ~4200 строк Go, 74 файла, 2 коммита («initial import» + CI).

## Вердикт

Это скелет SDS-проекта ~2016 года (лицензия «sdstack», copyright 2016-2018):
попытка переписать **Sheepdog** (распределённую блочную SDS для QEMU) на Go
с pluggable-архитектурой. Репозиторий **не собирается** и не собирался
никогда в текущем виде. Как основа «как есть» — не годится. Как **каркас
идей и интерфейсов** (backend/cache/cluster/hash registry) — полезен.

Связь с Sheepdog больше не актуальна: драйвер QEMU для sheepdog удалён
из upstream, протокол наследовать незачем (см. `roadmap.md` — решение
«greenfield, протокол свой»).

## Что хорошо ⭐

1. **Правильная модульная схема**: интерфейсы `backend.Backend`,
   `cache.Cache`, `cluster.Cluster`, `hash`, `journal`, `proxy` +
   registry (`Register*/New`) — хороший паттерн для ядра нового продукта.
2. **Идея KV-слоя** ([kv/storage.go](../kv/storage.go)): объект = имя +
   offset, параметризация `ndata/nparity` (копии/erasure) — правильная
   абстракция для распределённого блочного хранилища.
3. **Файловый backend** ([backend/filesystem/filesystem.go](../backend/filesystem/filesystem.go)):
   fallocate, weighted placement по дискам, веса из `statfs` — зачатки
   placement-логики.
4. **etcd embedded** ([cluster/etcdint/etcdint.go](../cluster/etcdint/etcdint.go)):
   идея встроенной consensus-основы для метаданных (в Vitastor аналог —
   встроенный etcd).
5. **Бенчмарки хешей** ([bench/hash/bench_test.go](../bench/hash/bench_test.go))
   — осознанный выбор быстрого хеша (xxhash) под placement.
6. fio-профиль ([tests/fio.txt](../tests/fio.txt)) — заготовка приёмочных
   тестов.

## Что плохо ❌

1. **Не компилируется**: нет `go.mod` (импорты `github.com/sdstack/storage`),
   отсутствует пакет `ring` (импортируется filesystem-backend), тип `Config`
   в sheepdog-прокси нигде не определён, поля `ProxySheepdog.weights/.ring`
   не объявлены, `api/api.go` целиком закомментирован, `journal.go` содержит
   методы от cache (copy-paste), зависимости (старый `coreos/etcd`,
   `OneOfOne/xxhash`, `goleveldb`) — древние, без go.sum.
2. **Sheepdog-прокси (1426 строк, ~34% кода)** — реализация мёртвого
   протокола: QEMU удалил sheepdog block driver; наследовать OID-протокол
   смысла нет. Удалить.
3. **CLI (`sds-storage/cmd/*`, ~40 файлов)** — почти все cobra-заглушки
   («volume create called»). Кода нет.
4. **Empty packages**: `cache/block`, `cache/filesystem` — по 1 строке.
5. **Нет тестов** на ядро (только бенчи хешей), нет CI сборки, нет go.mod,
   нет README/дока, нет версионирования.
6. **Нет распределённой логики**: ни репликации, ни recovery, ни object/VDI
   map, ни peer-обмена (из ~60 sheepdog-opcode реализовано ~8, без
   sheep2sheep), из transport — только TCP dialer-обёртка.
7. **Интерфейсы завязаны на `interface{}`-cfg + mapstructure** — без
   типизации и валидации; `Backend.WriteAt/ReadAt` без `context` и без
   векторных операций (нет `preadv/pwritev`, io_uring).
8. **Производительность архитектурно не готова к цели «не ниже Vitastor»**:
   открытие/закрытие файла на каждый ReadAt/WriteAt, синхронные syscall'ы,
   нет zero-copy, нет batch/async IO, аллокации на пути данных.

## Сравнение целей с реальностью

| Цель пользователя | В репо | Разрыв |
|---|---|---|
| Ceph-подобные CRUSH-крашмапы | только weighted ring (заглушка) | **закрыто вне репо**: [`crushkv`](https://github.com/unistack-org/crushkv) — byte-exact порт C-ядра Ceph на Go, golden-тесты, арена 0 B/op; интеграция в storage — обёртка |
| Go, скорость ≥ Vitastor | sync file IO per-op | с нуля: io_uring, zero-copy, no-alloc path, CPU affinity (приём арены доказан на placement) |
| Раздача как NVMe внутри ВМ | ничего | с нуля: vhost-user-blk server → QSD → vfio-user |
| Распределённость, реплики | заглушки | с нуля: репликация, quorum, recovery, failure domain |
| Метаданные/кластер | etcd embedded (заготовка) | переработать: MDS поверх etcd (external/embedded) |

## crushkv (срез аудита)

Репозиторий `crushkv` был битой реализацией идей CRUSH (свой формат карты,
несовместимый с Ceph; движок расходился с Ceph по шагам выбора).
Состояние после переработки:

- `crush/` — port C-ядра Ceph: все 5 алгоритмов bucket, choose/chooseleaf,
  tunables-профили, LN/jenkins — **byte-exact** (~6000 golden-векторов от
  настоящего Ceph-кода, 100%);
- `crushmap/` — парсеры: текст `*.crush`, JSON, **бинарный формат Ceph
  CrushWrapper** (decode+encode, байт-стабильный encode, круговые
  golden-тесты);
- GC: placement в mmap-арене, `DoRule` 225 OSD/straw2/rep3 = 34.5 мкс,
  **0 аллокаций** (`docs/GC_ARENA.md`);
- legacy-формат crushkv удалён, двоичный кодек теперь только Ceph-совместимый.

## Рекомендация

Не «чинить» репозиторий, а использовать его как место **нового greenfield
проекта**: удалить sheepdog-слой и CLI-заглушки, оставить/переработать
интерфейсы registry, завести `go.mod`, и строить по
[roadmap.md](roadmap.md) (фаза 0 — гигиена, дальше ядро, сеть, CRUSH,
data path, экспорт в ВМ).
