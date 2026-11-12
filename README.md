# unistack storage

Распределённая блочная СХД на чистом Go. Цели (зафиксированы владельцем):

1. **CRUSH-крашмапы как в Ceph** — размещение объектов по topology-карте
   (region → zone → rack → host → disk). Движок и формат карты: `crush/`
   (byte-exact порт C-ядра Ceph из репозитория
   [crushkv](https://github.com/unistack-org/crushkv), влит в этот
   репозиторий; golden-тесты против настоящего Ceph, 0 аллокаций на
   горячем пути, см. `docs/GC_ARENA.md`, лицензия — `LICENSE-crushkv`).
2. **Только Go, скорость не ниже [Vitastor](https://vitastor.io/docs/)**
   (~0.14 мс на 4K, миллионы IOPS на узел). Стратегия: минимум
   копирований/аллокаций, no-alloc data path, io_uring (Linux), свой
   бинарный протокол без gRPC/protobuf на data path.
3. **Диск внутри ВМ виден как NVMe-устройство.**

## Статус

- ✅ **MVP экспорта (фаза 4a)**: `pkg/nbd/` — NBD-сервер на stdlib Go +
  демон `cmd/unistord`. Проверено end-to-end: QEMU 11 отдаёт экспорт
  как `/dev/nvme0n1` гостю, mkfs/mount/запись работают, данные в
  backing-файле.
- ✅ **Экспорт-устройства**: `unistord` отдаёт том по NBD (TCP),
  VDUSE (`/dev/vduseX`, Linux) и vhost-user-blk (UNIX-сокет для
  QEMU/Dragonball); управление через REST `POST/GET/DELETE /v1/exports`
  и `unistorctl export …`.
- ✅ CRUSH: `crush/` (движок + формат карты Ceph: чтение/запись text,
  JSON, binary + арена без GC) — влит в этот репозиторий из crushkv.
- ✅ Метрики: `unistord -metrics 0.0.0.0:8080` → `GET /metrics` в формате
  Prometheus (скрейп VictoriaMetrics): состояние кластера, raft (состояние
  и участники), статус и диски всех OSD (в т.ч. удалённых — зонд StatFS),
  латентность/IOPS/BW, состояние PG и ёмкость томов, счётчики recovery.
  Единственный путь — `go.unistack.org/micro-meter-victoriametrics/v5`
  (реестр официальной библиотеки VictoriaMetrics), без prometheus-клиента.
  Экспорт только на отдельном `-metrics`-порту, REST API метрики не отдаёт.
  Детали: [`todos/runbook.md`](todos/runbook.md).
- ✅ Разделение сетей: у OSD второй слушатель для контроля
  (`-osd id@dir@data-addr@ctrl-addr`) — пинги/StatFS/health-пробы идут по нему,
  поэтому забитая дата-сеть не эвиктит живые OSD и не разваливает
  кластер. Простой запуск — `-data-dir <путь>` (id автоматически).
  Пустой ctrl-адрес = одна сеть (обратная совместимость).
- 🚧 Скелет репозитория: интерфейс-реестры `backend/`, `cache/`,
  `cluster/`, `hash/`, `journal/`, `transport/` (legacy-слой sheepdog-era
  удалён в фазе 0).
- 📋 Полный план: [`todos/roadmap.md`](todos/roadmap.md),
  аудит: [`todos/AUDIT.md`](todos/AUDIT.md).

## Модули

Единый Go-модуль `github.com/unistack-org/storage` (корень репозитория):

| Пакет / команда | Назначение |
|---|---|
| `cmd/unistord` | демон SDS: тома, кластер, REST admin API, экспорты (NBD/VDUSE/vhost-user-blk) |
| `cmd/unistorctl` | CLI: пулы, тома, кластер, `export create/ls/rm` |
| `cmd/sdsbench` | бенчмарк data path |
| `pkg/nbd` | NBD-сервер (stdlib, zero-alloc data path) |
| `pkg/vduse` | VDUSE (Linux) — userspace virtio-blk бэкенд |
| `pkg/vub` | vhost-user-blk бэкенд (UNIX-сокет, QEMU/Dragonball) |
| `pkg/store`, `pkg/object`, `pkg/volume`, … | ядро SDS |
| `crush/` | CRUSH-движок и крашмапы (порт C-ядра Ceph, ex-crushkv) |

## Сборка / тесты

```sh
GOWORK=off go build ./... && GOWORK=off go test -race ./...
```

`GOWORK=off` обязателен, если рядом лежит внешний `go.work`.

Быстрый старт:

```sh
GOWORK=off go build -o unistord ./cmd/unistord
./unistord -listen 127.0.0.1:10809 -export vol0=/tmp/vol0.img:1G
qemu-img info nbd://127.0.0.1:10809/vol0
```

Кластер без placement-флагов демона (политика размещения живёт в пулах):

```sh
./unistord -meta-dir /var/lib/unistord/meta \
  -data-dir /var/lib/unistord/data1 -data-dir /var/lib/unistord/data2 \
  -data-dir /var/lib/unistord/data3 -crushmap /etc/unistord/crush.map &
unistorctl pool create ssd -replicas 3 -pg-num 128   # политика пула
unistorctl vol create -pool ssd vm0 100G             # том наследует пул
./unistord ... -create ssd/vm0:100G       # то же при старте (-create shortcut)
unistorctl export create vm0                          # NBD; -type vduse|vhost-user-blk
```

В кластерном режиме `-export` нет: экспорт — объект времени работы
(`unistorctl export create` / REST `POST /v1/exports`), записи о нём
хранятся в meta и воспроизводятся после рестарта демона. `-export`
остаётся только в одтонном режиме (name=path[:size]).

`-data-dir` — простой способ поднять OSD: id назначается автоматически
(наименьший свободный, закрепляется в `<dir>/id`) и больше не указывается
вручную; `-epoch` не нужен — 0 = авто (1 на пустом кластере, далее публикация
пропускается). `-osd id@dir[@data-addr[@ctrl-addr]]` остаётся для полных
спецификаций: id из crushmap, адрес дата-сети и второй (ctrl-)адрес.
`-replicas/-pg-num/-min-size/-rule` демона — только дефолты для `-create`
(0/пусто = из snapshot кластера); пулам и `vol create -pool` они не нужны.

Защита от перепутанных дисков. Id закрепляется не только за каталогом,
но и за блочным устройством: при старте демон определяет стабильную
идентичность устройства каждого `-data-dir` (UUID ФС → серийник → WWID),
пишет её в `<dir>/device` и в `<meta-dir>/devices.json`, а при каждом
перезапуске сверяет живую идентичность с закреплённой. Если диски
смонтированы по именам ядра (`/dev/sdb`), а после перезагрузки порядок
опроса поменялся и диски поменялись mount-точками местами, демон
останавливается с fatal-ошибкой и объяснением — а не начинает молча
отдавать чужие объекты под другими id. Явная привязка: `-data-dir
uuid=<идентичность>@<каталог>`, где идентичность — из `unistorctl disk
probe <путь>` (он печатает UUID ФС, serial, wwid, как диск смонтирован
сейчас, и предупреждает, если fstab ссылается на диск по имени ядра);
для такой записи несовпадение — всегда fatal. Проверка работает и
кластерно: запись OSD в meta хранит устройство, и OSD откажется
стартовать под своим id на другом диске. Несколько каталогов на одном
устройстве — легальны (каждый сохраняет свой id); а вот «2 каталога на
диске A и 2 на диске B, ни один не закреплён» — однозначного
сопоставления нет, демон откажется стартовать и попросит передать
`-data-dir uuid=...@...`. Рекомендация: монтировать разделы по `UUID=`
в fstab.

